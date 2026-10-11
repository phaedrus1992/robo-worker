"""MCP tools, bearer-token auth, and the ASGI app."""

import hmac
import shutil
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import StrictInt
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from robo_worker.asker import Asker, AskError
from robo_worker.config import Config
from robo_worker.env import TagError
from robo_worker.health import ServiceState, health_report
from robo_worker.repos import RepoNameError
from robo_worker.scheduler import Scheduler, SubmitError
from robo_worker.states import IllegalTransition, JobState
from robo_worker.store import Job, StoreError, UnknownJob

INSTRUCTIONS = (
    "Run claude -p jobs on GitHub repo checkouts. Call submit_job, then poll get_job and "
    "events. A blocked job waits for resume_job with an answer."
)


class AuthError(Exception):
    """The request token does not allow this tool."""


# The SDK shows the text of a ToolError to the caller, and hides the text of any other
# exception. These are the failures a caller can act on, so their reason must reach it.
# Other exceptions are bugs: the SDK logs them with a traceback.
_CALLER_ERRORS = (
    AuthError,
    SubmitError,
    TagError,
    RepoNameError,
    IllegalTransition,
    UnknownJob,
    StoreError,
    AskError,
)


@contextmanager
def _tool_errors() -> Generator[None]:
    try:
        yield
    except _CALLER_ERRORS as exc:
        raise ToolError(str(exc)) from exc


@dataclass(frozen=True)
class Identity:
    kind: Literal["caller", "job"]
    name: str


type Resolver = Callable[[str], Identity | None]


def make_resolver(
    callers: Mapping[str, str], job_for_token: Callable[[str], str | None]
) -> Resolver:
    """Map a bearer token to a caller name or to the job id of a worker session."""
    known = [(token.encode(), name) for token, name in callers.items()]

    def resolve(token: str) -> Identity | None:
        raw = token.encode()
        for candidate, name in known:
            if hmac.compare_digest(candidate, raw):
                return Identity("caller", name)
        job_id = job_for_token(token)
        return Identity("job", job_id) if job_id else None

    return resolve


def bearer_token(value: str | None) -> str | None:
    if not value:
        return None
    scheme, _, token = value.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def _header(scope: Scope, name: bytes) -> str | None:
    for key, value in scope.get("headers", []):
        if key.lower() == name:
            return value.decode("latin-1")
    return None


class BearerAuth:
    """Refuse each HTTP request that has no known bearer token, except the open paths."""

    def __init__(
        self, app: ASGIApp, resolve: Resolver, open_paths: frozenset[str] = frozenset({"/health"})
    ) -> None:
        self._app = app
        self._resolve = resolve
        self._open_paths = open_paths

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "websocket":
            # No route uses a websocket. A close before accept refuses the handshake.
            await send({"type": "websocket.close", "code": 1008})
            return
        if scope["type"] == "http" and scope["path"] not in self._open_paths:
            token = bearer_token(_header(scope, b"authorization"))
            if token is None or self._resolve(token) is None:
                response = JSONResponse(
                    {"error": "unauthorized. Send the header Authorization: Bearer <token>."},
                    status_code=401,
                    headers={"WWW-Authenticate": "Bearer"},
                )
                await response(scope, receive, send)
                return
        await self._app(scope, receive, send)


def _identity(ctx: Context, resolve: Resolver) -> Identity:
    headers = ctx.headers or {}
    value = next((v for k, v in headers.items() if k.lower() == "authorization"), None)
    token = bearer_token(value)
    identity = resolve(token) if token else None
    if identity is None:
        raise AuthError("this request has no valid bearer token")
    return identity


def _caller(ctx: Context, resolve: Resolver) -> str:
    identity = _identity(ctx, resolve)
    if identity.kind != "caller":
        raise AuthError("this tool needs a caller token, not a job token")
    return identity.name


def _state(raw: str | None) -> JobState | None:
    if raw is None:
        return None
    try:
        return JobState(raw)
    except ValueError:
        raise SubmitError(f"state {raw!r} is not one of {[s.value for s in JobState]}") from None


def job_view(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "repo": job.repo,
        "state": job.state.value,
        "reason": job.reason,
        "caller": job.caller,
        "prompt": job.prompt,
        "tags": job.tags,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "interruptions": job.interruptions,
        "resets_at": job.resets_at,
        "result": job.result,
        "usage": job.usage,
    }


def _register_read_tools(mcp: MCPServer, scheduler: Scheduler, resolve: Resolver) -> None:
    store = scheduler.store

    @mcp.tool()
    async def get_job(job_id: str, ctx: Context) -> dict[str, Any]:
        """Return one job: state, reason, result, and usage."""
        with _tool_errors():
            _caller(ctx, resolve)
            return job_view(store.get(job_id))

    @mcp.tool()
    async def list_jobs(ctx: Context, state: str | None = None) -> dict[str, Any]:
        """Return all jobs, or the jobs in one state."""
        with _tool_errors():
            _caller(ctx, resolve)
            return {"jobs": [job_view(j) for j in store.all(_state(state))]}

    @mcp.tool()
    # StrictInt: the SDK would otherwise turn `true` into the cursor 1.
    async def events(job_id: str, ctx: Context, since: StrictInt = 0) -> dict[str, Any]:
        """Return the job events after cursor `since`, and the next cursor."""
        with _tool_errors():
            _caller(ctx, resolve)
            if since < 0:
                raise SubmitError(f"since must be a cursor of 0 or more, got {since!r}")
            items, cursor = store.events(job_id, since)
            return {"events": items, "next": cursor}


def _register_job_tools(mcp: MCPServer, scheduler: Scheduler, resolve: Resolver) -> None:
    @mcp.tool()
    async def submit_job(
        repo: str, prompt: str, ctx: Context, tags: list[str] | None = None
    ) -> dict[str, Any]:
        """Queue a claude -p job on a GitHub repo given as owner/name. Returns the job."""
        with _tool_errors():
            return job_view(scheduler.submit(repo, prompt, tags, _caller(ctx, resolve)))

    @mcp.tool()
    async def resume_job(job_id: str, answer: str, ctx: Context) -> dict[str, Any]:
        """Continue a blocked job with the answer to its question."""
        with _tool_errors():
            _caller(ctx, resolve)
            return job_view(scheduler.resume(job_id, answer))

    @mcp.tool()
    async def cancel_job(job_id: str, ctx: Context) -> dict[str, Any]:
        """Stop a running job, or remove a queued or paused job."""
        with _tool_errors():
            _caller(ctx, resolve)
            return job_view(scheduler.cancel(job_id))

    @mcp.tool()
    async def resume_queue(repo: str, ctx: Context) -> dict[str, Any]:
        """Start the paused queue of a repo again, after its checkout is clean."""
        with _tool_errors():
            _caller(ctx, resolve)
            scheduler.resume_queue(repo)
            return {"repo": repo, "paused": False}


def _register_other_tools(
    mcp: MCPServer, scheduler: Scheduler, asker: Asker, resolve: Resolver
) -> None:
    @mcp.tool()
    async def ask(prompt: str, ctx: Context, system: str | None = None) -> dict[str, Any]:
        """One text-only claude turn: no tools, no repo, no queue. Returns text and usage."""
        with _tool_errors():
            result = await asker.ask(prompt, system, _caller(ctx, resolve))
            return {"text": result.text, "usage": result.usage}

    @mcp.tool()
    async def report_progress(phase: str, detail: str, ctx: Context) -> dict[str, Any]:
        """For worker sessions only: record that a new phase of the job starts now."""
        with _tool_errors():
            identity = _identity(ctx, resolve)
            if identity.kind != "job":
                raise AuthError("report_progress needs the job token from the worker config")
            scheduler.report_progress(identity.name, phase, detail)
            return {"recorded": True}


def build_app(
    cfg: Config, scheduler: Scheduler, asker: Asker, callers: Mapping[str, str]
) -> ASGIApp:
    """Return the ASGI app: /mcp (bearer token required) and /health (open)."""
    resolve = make_resolver(callers, scheduler.job_for_progress_token)
    mcp = MCPServer(name="robo-worker", instructions=INSTRUCTIONS)
    _register_read_tools(mcp, scheduler, resolve)
    _register_job_tools(mcp, scheduler, resolve)
    _register_other_tools(mcp, scheduler, asker, resolve)

    @mcp.custom_route("/health", methods=["GET"])
    async def health(request: Request) -> JSONResponse:
        state = ServiceState(scheduler.pauses, scheduler.running_count, scheduler.store.unreadable)
        report = health_report(cfg, state, today=date.today(), which=shutil.which)
        return JSONResponse(report, status_code=200 if report["ok"] else 503)

    inner = mcp.streamable_http_app(
        json_response=True,
        stateless_http=True,
        transport_security=TransportSecuritySettings(allowed_hosts=list(cfg.allowed_hosts)),
        host=cfg.listen_host,
    )
    return BearerAuth(inner, resolve)
