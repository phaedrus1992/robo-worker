import json
import os
import stat
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import anyio
import pytest

from robo_worker import scheduler as scheduler_module
from robo_worker.checkout import DirtyCheckout
from robo_worker.config import Config
from robo_worker.env import TagError
from robo_worker.repos import RepoName, RepoNameError
from robo_worker.runner import CONTINUE_PROMPT
from robo_worker.scheduler import MAX_DETAIL_CHARS, Scheduler, SubmitError
from robo_worker.states import JobState
from robo_worker.store import Job, JobStore
from tests.conftest import FIXTURES, make_origin

pytestmark = [pytest.mark.anyio, pytest.mark.usefixtures("isolated_git")]


class Clock:
    def __init__(self) -> None:
        self.now = 1_000_000.0

    def __call__(self) -> float:
        return self.now


type SetEnv = Callable[..., None]


@pytest.fixture
def set_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> SetEnv:
    """Set the fake binaries' env. Call it before the Scheduler is built."""

    def apply(*fixtures: str, **extra: str) -> None:
        paths = os.pathsep.join(str(FIXTURES / name) for name in fixtures)
        monkeypatch.setenv("FAKE_CLAUDE_FIXTURES", paths)
        monkeypatch.setenv("FAKE_CLAUDE_RECORD", str(tmp_path / "record.jsonl"))
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-or-v1-must-not-leak")
        monkeypatch.setenv("ANTHROPIC_BASE_URL", "https://openrouter.ai/api")
        for name, value in extra.items():
            monkeypatch.setenv(name, value)

    return apply


@pytest.fixture
def store(config: Config) -> JobStore:
    return JobStore(config.state_root)


@asynccontextmanager
async def running(scheduler: Scheduler) -> AsyncGenerator[None]:
    async with anyio.create_task_group() as tg:
        tg.start_soon(scheduler.run_forever)
        yield
        tg.cancel_scope.cancel()


async def wait_state(store: JobStore, job_id: str, *states: JobState) -> Job:
    with anyio.fail_after(15):
        while True:
            job = store.get(job_id)
            if job.state in states:
                return job
            await anyio.sleep(0.05)


def records(tmp_path: Path) -> list[dict[str, Any]]:
    path = tmp_path / "record.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


async def test_job_runs_to_done(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    make_origin(tmp_path)
    export = tmp_path / "export.sh"
    export.write_text("export ANTHROPIC_BASE_URL='https://openrouter.ai/api'\n")
    set_env("success.jsonl", FAKE_LLMENV_OUTPUT=str(export))
    scheduler = Scheduler(config, store)
    job = scheduler.submit("o/r", "do it", ["lang-python"], "agent")
    async with running(scheduler):
        done = await wait_state(store, job.id, JobState.DONE)
    assert done.result is not None
    assert done.result["pr_url"] == "https://github.com/o/r/pull/1"
    (call,) = records(tmp_path)
    assert call["cwd"] == str(config.workspace_root / "o" / "r")
    assert call["stdin"] == "do it"
    assert "CLAUDE_CODE_OAUTH_TOKEN" in call["env"]
    assert not [name for name in call["env"] if name.startswith("ANTHROPIC_")]
    usage = [json.loads(line) for line in store.usage_path.read_text().splitlines()]
    assert usage[0]["job_id"] == job.id
    assert usage[0]["caller"] == "agent"
    assert usage[0]["cost_usd"] == 0.25
    kinds = [e["kind"] for e in store.events(job.id, 0)[0]]
    assert kinds[0] == "submitted"
    assert {"started", "rate_limit", "finished"} <= set(kinds)
    mcp = store.job_dir(job.id) / "mcp.json"
    assert stat.S_IMODE(mcp.stat().st_mode) == 0o600
    server = json.loads(mcp.read_text())["mcpServers"]["robo-worker"]
    assert server["url"] == "http://127.0.0.1:8765/mcp"
    assert scheduler.job_for_progress_token(server["headers"]["Authorization"][7:]) is None


async def test_blocked_then_resumed(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    make_origin(tmp_path)
    set_env("blocked.jsonl", "success.jsonl")
    scheduler = Scheduler(config, store)
    job = scheduler.submit("o/r", "do it", None, "mac")
    async with running(scheduler):
        blocked = await wait_state(store, job.id, JobState.BLOCKED)
        assert blocked.reason == "Which base branch?"
        scheduler.resume(job.id, "use main")
        await wait_state(store, job.id, JobState.DONE)
    second = records(tmp_path)[1]
    assert second["stdin"] == "use main"
    assert second["argv"][second["argv"].index("--resume") + 1] == job.session_id


async def test_resume_refuses_a_job_that_is_not_blocked(
    config: Config, store: JobStore, set_env: SetEnv
) -> None:
    set_env("success.jsonl")
    scheduler = Scheduler(config, store)
    job = scheduler.submit("o/r", "do it", None, "mac")
    with pytest.raises(SubmitError, match="not blocked"):
        scheduler.resume(job.id, "x")


async def test_rate_limit_pauses_all_queues(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    make_origin(tmp_path, "o", "r")
    make_origin(tmp_path, "o", "s")
    set_env("rate_limited.jsonl", "success.jsonl")
    clock = Clock()
    scheduler = Scheduler(config, store, clock=clock)
    first = scheduler.submit("o/r", "first", None, "agent")
    async with running(scheduler):
        limited = await wait_state(store, first.id, JobState.RATE_LIMITED)
        assert limited.resets_at == 1791871200.0
        second = scheduler.submit("o/s", "second", None, "agent")
        await anyio.sleep(0.5)
        assert store.get(second.id).state is JobState.QUEUED
        clock.now = 1791871201.0
        scheduler.kick()
        await wait_state(store, first.id, JobState.DONE)
        await wait_state(store, second.id, JobState.DONE)
    resumed = next(r for r in records(tmp_path)[1:] if "--resume" in r["argv"])
    assert resumed["stdin"] == CONTINUE_PROMPT


async def test_dirty_checkout_pauses_the_repo(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    make_origin(tmp_path)
    set_env("success.jsonl", FAKE_CLAUDE_TOUCH="scratch.txt")
    scheduler = Scheduler(config, store)
    first = scheduler.submit("o/r", "one", None, "agent")
    async with running(scheduler):
        await wait_state(store, first.id, JobState.DONE)
        await anyio.sleep(0.2)
        assert scheduler.pauses.repos["o/r"].startswith("checkout_dirty")
        second = scheduler.submit("o/r", "two", None, "agent")
        await anyio.sleep(0.5)
        assert store.get(second.id).state is JobState.QUEUED
        (config.workspace_root / "o" / "r" / "scratch.txt").unlink()
        scheduler.resume_queue("o/r")
        await wait_state(store, second.id, JobState.DONE)
    with pytest.raises(SubmitError, match="not paused"):
        scheduler.resume_queue("o/other")


async def test_recover_after_restart(config: Config, store: JobStore, set_env: SetEnv) -> None:
    set_env("success.jsonl")
    clock = Clock()
    running_job = store.create(repo="o/r", prompt="p", tags=[], caller="agent")
    store.transition(running_job, JobState.RUNNING)
    worn_out = store.create(repo="o/s", prompt="p", tags=[], caller="agent")
    worn_out.interruptions = config.max_interruptions
    store.transition(worn_out, JobState.RUNNING)
    blocked = store.create(repo="o/t", prompt="p", tags=[], caller="agent")
    store.transition(blocked, JobState.RUNNING)
    store.transition(blocked, JobState.BLOCKED, "question")
    limited = store.create(repo="o/u", prompt="p", tags=[], caller="agent")
    store.transition(limited, JobState.RUNNING)
    limited.resets_at = clock.now + 100
    store.transition(limited, JobState.RATE_LIMITED, "limit")

    scheduler = Scheduler(config, store, clock=clock)
    scheduler.recover()

    requeued = store.get(running_job.id)
    assert (requeued.state, requeued.interruptions) == (JobState.QUEUED, 1)
    failed = store.get(worn_out.id)
    assert failed.state is JobState.FAILED
    assert failed.reason is not None
    assert "interrupted 3 times" in failed.reason
    assert store.get(blocked.id).state is JobState.BLOCKED
    assert store.get(limited.id).state is JobState.RATE_LIMITED
    assert scheduler.pauses.rate_limit_until == clock.now + 100


async def test_submit_refuses_bad_input(config: Config, store: JobStore, set_env: SetEnv) -> None:
    set_env("success.jsonl")
    scheduler = Scheduler(config, store)
    with pytest.raises(SubmitError, match="deny_tags"):
        scheduler.submit("o/r", "p", ["blocked-tag"], "agent")
    with pytest.raises(TagError):
        scheduler.submit("o/r", "p", ["a,b"], "agent")
    with pytest.raises(SubmitError, match="prompt is empty"):
        scheduler.submit("o/r", "  ", None, "agent")
    with pytest.raises(RepoNameError):
        scheduler.submit("../r", "p", None, "agent")


async def test_denied_bundle_fails_before_claude(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    make_origin(tmp_path)
    export = tmp_path / "export.sh"
    export.write_text("export LLMENV_ACTIVE_BUNDLES='base,ben-style'\n")
    set_env("success.jsonl", FAKE_LLMENV_OUTPUT=str(export))
    scheduler = Scheduler(config, store)
    job = scheduler.submit("o/r", "p", None, "agent")
    async with running(scheduler):
        failed = await wait_state(store, job.id, JobState.FAILED)
    assert failed.reason is not None
    assert "ben-style" in failed.reason
    assert records(tmp_path) == []


async def test_llmenv_failure_names_the_tags(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    make_origin(tmp_path)
    set_env("success.jsonl", FAKE_LLMENV_EXIT="2")
    scheduler = Scheduler(config, store)
    job = scheduler.submit("o/r", "p", ["lang-python"], "agent")
    async with running(scheduler):
        failed = await wait_state(store, job.id, JobState.FAILED)
    assert failed.reason is not None
    assert "lang-python" in failed.reason


async def test_auth_error_pauses_every_queue(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    make_origin(tmp_path)
    set_env("auth_error.jsonl", "success.jsonl")
    scheduler = Scheduler(config, store)
    first = scheduler.submit("o/r", "p", None, "agent")
    async with running(scheduler):
        await wait_state(store, first.id, JobState.FAILED)
        second = scheduler.submit("o/r", "p", None, "agent")
        await anyio.sleep(0.5)
        assert store.get(second.id).state is JobState.QUEUED
    assert scheduler.pauses.auth is not None
    assert "401" in scheduler.pauses.auth


async def test_missing_token_file_pauses_and_keeps_the_job(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    make_origin(tmp_path)
    set_env("success.jsonl")
    config.oauth_token_file.unlink()
    scheduler = Scheduler(config, store)
    job = scheduler.submit("o/r", "p", None, "agent")
    async with running(scheduler):
        await anyio.sleep(0.5)
    assert store.get(job.id).state is JobState.QUEUED
    assert scheduler.pauses.auth is not None
    assert "cannot read" in scheduler.pauses.auth


async def test_cancel_queued_and_running(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    make_origin(tmp_path)
    set_env("crash.jsonl", FAKE_CLAUDE_SLEEP="30")
    scheduler = Scheduler(config, store)
    first = scheduler.submit("o/r", "one", None, "agent")
    second = scheduler.submit("o/r", "two", None, "agent")
    async with running(scheduler):
        await wait_state(store, first.id, JobState.RUNNING)
        assert store.get(second.id).state is JobState.QUEUED
        assert scheduler.cancel(second.id).state is JobState.CANCELLED
        scheduler.cancel(first.id)
        await wait_state(store, first.id, JobState.CANCELLED)
    with pytest.raises(SubmitError, match="already"):
        scheduler.cancel(second.id)


async def test_parallel_limit(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    make_origin(tmp_path, "o", "r")
    make_origin(tmp_path, "o", "s")
    set_env("success.jsonl", FAKE_CLAUDE_SLEEP="1")
    scheduler = Scheduler(replace(config, max_parallel_jobs=1), store)
    first = scheduler.submit("o/r", "one", None, "agent")
    second = scheduler.submit("o/s", "two", None, "agent")
    async with running(scheduler):
        await wait_state(store, first.id, JobState.RUNNING)
        assert store.get(second.id).state is JobState.QUEUED
        await wait_state(store, second.id, JobState.DONE)


async def test_progress_tokens(config: Config, store: JobStore, set_env: SetEnv) -> None:
    set_env("success.jsonl")
    scheduler = Scheduler(config, store)
    job = scheduler.submit("o/r", "p", None, "agent")
    token = scheduler.issue_progress_token(job.id)
    assert scheduler.job_for_progress_token(token) == job.id
    scheduler.report_progress(job.id, "branch", "x" * (MAX_DETAIL_CHARS + 10))
    event = store.events(job.id, 0)[0][-1]
    assert event["kind"] == "progress"
    assert len(event["data"]["detail"]) == MAX_DETAIL_CHARS
    with pytest.raises(SubmitError, match="phase is empty"):
        scheduler.report_progress(job.id, " ", "x")
    scheduler.revoke_progress_token(token)
    assert scheduler.job_for_progress_token(token) is None


async def test_cancel_during_a_dirty_prepare_is_not_lost(
    config: Config, store: JobStore, set_env: SetEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    set_env("success.jsonl")
    entered, release = anyio.Event(), anyio.Event()

    async def blocked_checkout(
        git_bin: str, root: Path, repo: RepoName, template: str, timeout: float
    ) -> Path:
        entered.set()
        await release.wait()
        raise DirtyCheckout("dirty on purpose")

    monkeypatch.setattr(scheduler_module, "prepare_checkout", blocked_checkout)
    scheduler = Scheduler(config, store)
    job = scheduler.submit("o/r", "p", None, "agent")
    async with running(scheduler):
        with anyio.fail_after(5):
            await entered.wait()
        scheduler.cancel(job.id)
        release.set()
        cancelled = await wait_state(store, job.id, JobState.CANCELLED)
    assert cancelled.reason == "cancelled by a caller"


async def test_a_corrupt_job_does_not_stop_the_queue(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    make_origin(tmp_path)
    set_env("success.jsonl")
    broken = store.create(repo="o/r", prompt="p", tags=[], caller="agent")
    (store.job_dir(broken.id) / "job.json").write_text("{not json")
    scheduler = Scheduler(config, store)
    scheduler.recover()
    job = scheduler.submit("o/r", "do it", None, "agent")
    async with running(scheduler):
        await wait_state(store, job.id, JobState.DONE)
    assert store.unreadable == {broken.id}


async def test_cancel_refuses_a_finished_job(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    set_env("success.jsonl")
    scheduler = Scheduler(config, store)
    job = scheduler.submit("o/r", "p", None, "agent")
    scheduler._cancels[job.id] = anyio.Event()
    store.transition(job, JobState.CANCELLED, "done elsewhere")
    with pytest.raises(SubmitError, match="already"):
        scheduler.cancel(job.id)


async def test_auth_pause_is_an_event(
    tmp_path: Path, config: Config, store: JobStore, set_env: SetEnv
) -> None:
    make_origin(tmp_path)
    set_env("success.jsonl")
    config.oauth_token_file.unlink()
    scheduler = Scheduler(config, store)
    job = scheduler.submit("o/r", "p", None, "agent")
    async with running(scheduler):
        with anyio.fail_after(10):
            while scheduler.pauses.auth is None:
                await anyio.sleep(0.05)
    kinds = [e["kind"] for e in store.events(job.id, 0)[0]]
    assert "queue_paused" in kinds
