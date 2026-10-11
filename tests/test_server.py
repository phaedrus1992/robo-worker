import json
import os
from collections.abc import AsyncGenerator, MutableMapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import anyio
import httpx2
import pytest
import uvicorn
from hypothesis import given
from hypothesis import strategies as st
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

from robo_worker.asker import Asker
from robo_worker.config import Config
from robo_worker.scheduler import Scheduler
from robo_worker.server import BearerAuth, bearer_token, build_app
from robo_worker.states import JobState
from robo_worker.store import JobStore
from tests.conftest import AGENT_TOKEN, FIXTURES, make_origin

pytestmark = [pytest.mark.anyio, pytest.mark.usefixtures("isolated_git")]


@pytest.fixture
def stack(config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Scheduler, Any]:
    fixtures = os.pathsep.join(str(FIXTURES / f) for f in ("success.jsonl",))
    monkeypatch.setenv("FAKE_CLAUDE_FIXTURES", fixtures)
    monkeypatch.setenv("FAKE_CLAUDE_RECORD", str(tmp_path / "record.jsonl"))
    store = JobStore(config.state_root)
    scheduler = Scheduler(config, store)
    asker = Asker(config, store, scheduler.pauses)
    callers = {AGENT_TOKEN: "agent"}
    return scheduler, build_app(config, scheduler, asker, callers)


@asynccontextmanager
async def serving(app: Any, scheduler: Scheduler) -> AsyncGenerator[str]:
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on")
    )
    stopped = anyio.Event()

    async def serve() -> None:
        try:
            await server.serve()
        finally:
            stopped.set()

    async with anyio.create_task_group() as tg:
        tg.start_soon(scheduler.run_forever)
        tg.start_soon(serve)
        with anyio.fail_after(10):
            while not server.started:
                await anyio.sleep(0.02)
        port = server.servers[0].sockets[0].getsockname()[1]
        try:
            yield f"http://127.0.0.1:{port}"
        finally:
            # A graceful stop closes the sockets; a bare cancel leaks them.
            server.should_exit = True
            with anyio.fail_after(10):
                await stopped.wait()
            tg.cancel_scope.cancel()


@asynccontextmanager
async def mcp_client(base: str, token: str) -> AsyncGenerator[Client]:
    http = httpx2.AsyncClient(headers={"Authorization": f"Bearer {token}"}, timeout=30)
    async with http, Client(streamable_http_client(f"{base}/mcp", http_client=http)) as client:
        yield client


def data(result: Any) -> dict[str, Any]:
    assert not result.is_error, result.content
    content = result.structured_content
    if isinstance(content, dict):
        return content["result"] if set(content) == {"result"} else content
    return json.loads(result.content[0].text)


def error_text(result: Any) -> str:
    assert result.is_error
    return " ".join(getattr(block, "text", "") for block in result.content)


def test_bearer_token_parsing() -> None:
    assert bearer_token("Bearer abc") == "abc"
    assert bearer_token("bearer abc") == "abc"
    assert bearer_token("Basic abc") is None
    assert bearer_token("Bearer ") is None
    assert bearer_token(None) is None


async def test_health_needs_no_token(stack: tuple[Scheduler, Any]) -> None:
    scheduler, app = stack
    async with serving(app, scheduler) as base, httpx2.AsyncClient() as http:
        response = await http.get(f"{base}/health")
    assert response.status_code == 200
    assert response.json()["ok"] is True


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong"}])
async def test_mcp_refuses_bad_tokens(
    stack: tuple[Scheduler, Any], headers: dict[str, str]
) -> None:
    scheduler, app = stack
    async with serving(app, scheduler) as base, httpx2.AsyncClient() as http:
        response = await http.post(f"{base}/mcp", headers=headers, json={})
    assert response.status_code == 401


async def test_foreign_host_header_is_refused(stack: tuple[Scheduler, Any]) -> None:
    scheduler, app = stack
    headers = {"Authorization": f"Bearer {AGENT_TOKEN}", "Host": "evil.example:80"}
    async with serving(app, scheduler) as base, httpx2.AsyncClient() as http:
        response = await http.post(f"{base}/mcp", headers=headers, json={})
    assert response.status_code >= 400
    assert response.status_code != 401


async def test_submit_runs_and_records_the_caller(
    stack: tuple[Scheduler, Any], tmp_path: Path
) -> None:
    make_origin(tmp_path)
    scheduler, app = stack
    async with serving(app, scheduler) as base, mcp_client(base, AGENT_TOKEN) as client:
        job = data(await client.call_tool("submit_job", {"repo": "o/r", "prompt": "do it"}))
        with anyio.fail_after(15):
            while (view := data(await client.call_tool("get_job", {"job_id": job["id"]})))[
                "state"
            ] != "done":
                await anyio.sleep(0.1)
        assert view["caller"] == "agent"
        events = data(await client.call_tool("events", {"job_id": job["id"], "since": 0}))
        assert events["events"][-1]["kind"] == "finished"
        listed = data(await client.call_tool("list_jobs", {"state": "done"}))
        assert [j["id"] for j in listed["jobs"]] == [job["id"]]
        bad = await client.call_tool("list_jobs", {"state": "nope"})
        assert "not one of" in error_text(bad)
        unknown = await client.call_tool("get_job", {"job_id": "0" * 32})
        assert "no job has id" in error_text(unknown)


async def test_job_token_only_reports_progress(stack: tuple[Scheduler, Any]) -> None:
    scheduler, app = stack
    job = scheduler.submit("o/r", "p", None, "agent")
    token = scheduler.issue_progress_token(job.id)
    async with serving(app, scheduler) as base:
        async with mcp_client(base, token) as worker:
            recorded = await worker.call_tool(
                "report_progress", {"phase": "branch", "detail": "feat/x"}
            )
            assert data(recorded) == {"recorded": True}
            refused = await worker.call_tool("get_job", {"job_id": job.id})
            assert "caller token" in error_text(refused)
        async with mcp_client(base, AGENT_TOKEN) as caller:
            refused = await caller.call_tool("report_progress", {"phase": "x", "detail": "y"})
            assert "job token" in error_text(refused)
    progress = [e for e in scheduler.store.events(job.id, 0)[0] if e["kind"] == "progress"]
    assert progress[0]["data"] == {"phase": "branch", "detail": "feat/x"}
    assert scheduler.store.get(job.id).state in (JobState.QUEUED, JobState.FAILED)


async def test_ask_tool(
    stack: tuple[Scheduler, Any], monkeypatch: pytest.MonkeyPatch, config: Config
) -> None:
    monkeypatch.setenv("FAKE_CLAUDE_FIXTURES", str(FIXTURES / "ask_success.jsonl"))
    store = JobStore(config.state_root)
    scheduler = Scheduler(config, store)
    asker = Asker(config, store, scheduler.pauses)
    app = build_app(config, scheduler, asker, {AGENT_TOKEN: "agent"})
    async with serving(app, scheduler) as base, mcp_client(base, AGENT_TOKEN) as client:
        answer = data(await client.call_tool("ask", {"prompt": "say hello"}))
    assert answer["text"] == "hello"


async def test_events_refuse_a_boolean_cursor(stack: tuple[Scheduler, Any]) -> None:
    scheduler, app = stack
    job = scheduler.submit("o/r", "p", None, "agent")
    async with serving(app, scheduler) as base, mcp_client(base, AGENT_TOKEN) as client:
        refused = await client.call_tool("events", {"job_id": job.id, "since": True})
    assert "since" in error_text(refused)


async def test_corrupt_job_reason_reaches_the_caller(stack: tuple[Scheduler, Any]) -> None:
    scheduler, app = stack
    job = scheduler.store.create(repo="o/r", prompt="p", tags=[], caller="agent")
    (scheduler.store.job_dir(job.id) / "job.json").write_text("{not json")
    async with serving(app, scheduler) as base, mcp_client(base, AGENT_TOKEN) as client:
        refused = await client.call_tool("get_job", {"job_id": job.id})
    assert "Fix or move the file" in error_text(refused)


async def test_websocket_is_refused_before_accept() -> None:
    sent: list[MutableMapping[str, Any]] = []

    async def inner(scope: Any, receive: Any, send: Any) -> None:
        raise AssertionError("the inner app must not see a websocket")

    async def receive() -> dict[str, Any]:
        return {"type": "websocket.connect"}

    async def send(message: MutableMapping[str, Any]) -> None:
        sent.append(message)

    auth = BearerAuth(inner, lambda token: None)
    await auth({"type": "websocket", "path": "/mcp", "headers": []}, receive, send)
    assert sent == [{"type": "websocket.close", "code": 1008}]


@given(st.text(max_size=40))
def test_bearer_token_never_raises(value: str) -> None:
    token = bearer_token(value)
    if token is not None:
        assert token
        assert token == token.strip()
        assert token in value


@given(st.text(alphabet=st.characters(exclude_categories=("Z", "C")), min_size=1, max_size=40))
def test_bearer_prefix_round_trips(token: str) -> None:
    assert bearer_token("Bearer " + token) == token
