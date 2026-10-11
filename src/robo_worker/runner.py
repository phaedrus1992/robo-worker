"""Run one claude -p process and turn its stream into a RunOutcome."""

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Literal, cast

import anyio
from anyio import CancelScope
from anyio.abc import Process

from robo_worker.config import Config
from robo_worker.stream import InitEvent, RateLimitEvent, ResultEvent, StreamError, parse_line

_log = logging.getLogger(__name__)

# One line holds one event. A large tool result fits; a runaway process does not.
MAX_LINE_BYTES = 64 * 1024 * 1024
CONTINUE_PROMPT = "Continue the task from the point where you stopped."
STATUS_VALUES = frozenset({"done", "blocked", "failed"})
_HTTP_TOO_MANY = 429
AUTH_HTTP_STATUSES = frozenset({401, 403})

RESULT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["done", "blocked", "failed"]},
        "summary": {"type": "string"},
        "pr_url": {"type": "string"},
        "issues_filed": {"type": "array", "items": {"type": "string"}},
        "blocked_reason": {"type": "string"},
    },
    "required": ["status", "summary", "issues_filed"],
    "additionalProperties": False,
}

WORKER_INSTRUCTIONS = """\
You are an unattended worker for robo-worker job {job_id}. Nobody reads your output until you stop.
At the start of each new phase of the work, call the report_progress tool of the robo-worker \
MCP server. Examples of phases: branch created, implementation done, review done, PR opened, \
CI green, CI red, merged.
If you need a decision from ranger, stop. Return status "blocked" and put the question in \
blocked_reason.
End with a result that matches the JSON schema. Put PR and issue URLs in pr_url and issues_filed.
"""

type OutcomeKind = Literal[
    "done", "blocked", "failed", "rate_limited", "auth", "timeout", "cancelled"
]
type EventSink = Callable[[str, dict[str, Any]], None]
type StopReason = Literal["exited", "timeout", "cancelled"]


@dataclass(frozen=True)
class RunSpec:
    job_id: str
    cwd: Path
    env: dict[str, str]
    prompt: str
    session_id: str
    resume: bool
    mcp_config_path: Path
    stream_path: Path
    stderr_path: Path


@dataclass(frozen=True)
class RunSettings:
    claude_bin: str
    max_seconds: float
    kill_grace_seconds: float
    allowed_key_sources: frozenset[str]
    deny_output_styles: frozenset[str]

    @classmethod
    def from_config(cls, cfg: Config) -> RunSettings:
        return cls(
            claude_bin=cfg.claude_bin,
            max_seconds=cfg.max_job_seconds,
            kill_grace_seconds=cfg.kill_grace_seconds,
            allowed_key_sources=frozenset(cfg.allowed_key_sources),
            deny_output_styles=frozenset(cfg.deny_output_styles),
        )


@dataclass(frozen=True)
class RunOutcome:
    kind: OutcomeKind
    reason: str
    result: dict[str, Any] | None = None
    usage: dict[str, Any] | None = None
    resets_at: float | None = None


@dataclass
class StreamState:
    init: InitEvent | None = None
    limited: RateLimitEvent | None = None
    result: ResultEvent | None = None
    violation: str | None = None
    stdin_error: str | None = None


def build_argv(claude_bin: str, spec: RunSpec) -> list[str]:
    """Return the claude argv. The prompt is not in it: it goes to stdin."""
    session = ["--resume", spec.session_id] if spec.resume else ["--session-id", spec.session_id]
    return [
        claude_bin,
        "-p",
        "--output-format",
        "stream-json",
        "--verbose",
        *session,
        "--permission-mode",
        "bypassPermissions",
        "--json-schema",
        json.dumps(RESULT_SCHEMA),
        "--append-system-prompt",
        WORKER_INSTRUCTIONS.format(job_id=spec.job_id),
        # Project and local settings come from the checkout, so a repo could set env such as
        # ANTHROPIC_BASE_URL there. The worker setup comes from llmenv, a user source.
        "--setting-sources",
        "user",
        # --mcp-config takes several values, so it stays last and nothing follows it.
        "--mcp-config",
        str(spec.mcp_config_path),
    ]


def check_policy(init: InitEvent, settings: RunSettings) -> str | None:
    """Return why the session must stop, or None when its auth and output style are allowed."""
    if init.api_key_source not in settings.allowed_key_sources:
        return (
            f"claude uses auth source {init.api_key_source!r}, not one of "
            f"{sorted(settings.allowed_key_sources)}. Remove API keys from the worker setup."
        )
    if init.output_style in settings.deny_output_styles:
        return (
            f"claude loaded the denied output style {init.output_style!r}. "
            "Change the job tags or the llmenv bundles."
        )
    return None


def validate_result(output: object) -> str | None:
    if not isinstance(output, dict):
        return "result has no structured output"
    status = output.get("status")
    if status not in STATUS_VALUES:
        return f"result status {status!r} is not one of {sorted(STATUS_VALUES)}"
    if not isinstance(output.get("summary"), str):
        return "result has no summary"
    if status == "blocked" and not output.get("blocked_reason"):
        return "blocked result has no blocked_reason"
    return None


def usage_of(result: ResultEvent) -> dict[str, Any]:
    return {
        "cost_usd": result.cost_usd,
        "usage": result.usage,
        "model_usage": result.model_usage,
        "session_id": result.session_id,
    }


def _limit_outcome(state: StreamState, usage: dict[str, Any] | None) -> RunOutcome:
    resets_at = state.limited.resets_at if state.limited else None
    return RunOutcome("rate_limited", "plan rate limit reached", usage=usage, resets_at=resets_at)


def _error_outcome(result: ResultEvent, state: StreamState, usage: dict[str, Any]) -> RunOutcome:
    if result.api_error_status == _HTTP_TOO_MANY or state.limited:
        return _limit_outcome(state, usage)
    if result.api_error_status in AUTH_HTTP_STATUSES:
        return RunOutcome(
            "auth", f"claude auth failed with HTTP {result.api_error_status}", usage=usage
        )
    return RunOutcome("failed", f"claude ended with result {result.subtype!r}", usage=usage)


def _no_result_reason(state: StreamState, exit_code: int | None) -> str:
    reason = (
        f"claude exited with code {exit_code} and gave no result. "
        "Read stderr.log and stream.jsonl in the job directory."
    )
    if state.stdin_error:
        reason += f" The prompt did not reach claude: {state.stdin_error}"
    return reason


def _stream_failure(state: StreamState, exit_code: int | None) -> RunOutcome | None:
    """Return the outcome when the stream stops the run before a usable result."""
    if state.violation:
        return RunOutcome("failed", state.violation)
    if state.init is None:
        # The policy check reads the init event, so without it the auth source is unknown.
        return RunOutcome("failed", NO_INIT_REASON)
    if state.result is None:
        if state.limited:
            return _limit_outcome(state, None)
        return RunOutcome("failed", _no_result_reason(state, exit_code))
    return None


def outcome_from(state: StreamState, exit_code: int | None) -> RunOutcome:
    """Map what the stream showed, and the exit code, to one outcome."""
    failure = _stream_failure(state, exit_code)
    if failure is not None:
        return failure
    result = state.result
    if result is None:
        raise AssertionError("_stream_failure returns an outcome when there is no result")
    usage = usage_of(result)
    if result.is_error or result.subtype != "success":
        return _error_outcome(result, state, usage)
    output = result.structured_output
    error = validate_result(output)
    if error or output is None:
        return RunOutcome("failed", error or "result has no structured output", usage=usage)
    reason = output["blocked_reason"] if output["status"] == "blocked" else output["summary"]
    return RunOutcome(cast(OutcomeKind, output["status"]), reason, result=output, usage=usage)


NO_INIT_REASON = "claude gave no init event, so its auth source and output style are unknown"


def apply_line(line: str, state: StreamState, settings: RunSettings, on_event: EventSink) -> None:
    """Update ``state`` from one stream line, and report rate-limit and parse events."""
    try:
        event = parse_line(line)
    except StreamError as exc:
        on_event("stream_error", {"error": str(exc)})
        return
    if isinstance(event, InitEvent):
        state.init = event
        state.violation = check_policy(event, settings)
    elif event is not None and state.init is None:
        state.violation = NO_INIT_REASON
    elif isinstance(event, RateLimitEvent):
        data = {"status": event.status, "resets_at": event.resets_at, "type": event.limit_type}
        on_event("rate_limit", data)
        if event.limited:
            state.limited = event
    elif isinstance(event, ResultEvent):
        state.result = event


class _Supervisor:
    """Read one process's stream, and stop the process on timeout, cancel, or a violation."""

    def __init__(
        self, proc: Process, spec: RunSpec, settings: RunSettings, on_event: EventSink
    ) -> None:
        self.proc = proc
        self.spec = spec
        self.settings = settings
        self.on_event = on_event
        self.state = StreamState()
        self.stop_reason: StopReason = "exited"

    async def run(self, cancel: anyio.Event) -> None:
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._watch_cancel, cancel, tg.cancel_scope)
                # The send runs beside the reader, so a process that never reads stdin cannot
                # block the job past max_seconds.
                tg.start_soon(self._send_prompt)
                with anyio.move_on_after(self.settings.max_seconds) as timer:
                    await self._read_stream()
                    if self.state.violation is None:
                        await self.proc.wait()
                if timer.cancelled_caught:
                    self.stop_reason = "timeout"
                tg.cancel_scope.cancel()
        finally:
            # Also on an error in this class: nothing else limits the process after this.
            with CancelScope(shield=True):
                await self._stop()

    async def _send_prompt(self) -> None:
        stdin = self.proc.stdin
        if stdin is None:
            raise RuntimeError("claude process has no stdin pipe")
        try:
            await stdin.send(self.spec.prompt.encode("utf-8"))
            await stdin.aclose()
        except (anyio.BrokenResourceError, OSError) as exc:
            # claude exited before it read the prompt; the exit code tells the rest.
            self.state.stdin_error = str(exc) or type(exc).__name__
            self.on_event("stdin_error", {"error": self.state.stdin_error})

    async def _watch_cancel(self, cancel: anyio.Event, scope: CancelScope) -> None:
        await cancel.wait()
        self.stop_reason = "cancelled"
        scope.cancel()

    def _consume(self, raw: bytes, sink: IO[bytes]) -> bool:
        sink.write(raw + b"\n")
        sink.flush()
        if raw.strip():
            text = raw.decode("utf-8", errors="replace")
            apply_line(text, self.state, self.settings, self.on_event)
        return self.state.violation is not None

    async def _read_stream(self) -> None:
        if self.proc.stdout is None:
            raise RuntimeError("claude process has no stdout pipe")
        pending = bytearray()
        with self.spec.stream_path.open("ab") as sink:
            async for chunk in self.proc.stdout:
                pending.extend(chunk)
                *lines, rest = pending.split(b"\n")
                pending = bytearray(rest)
                if any(self._consume(bytes(raw), sink) for raw in lines):
                    return
                if len(pending) > MAX_LINE_BYTES:
                    self.state.violation = f"a stream line is longer than {MAX_LINE_BYTES} bytes"
                    return
            if pending.strip():
                self._consume(bytes(pending), sink)

    async def _stop(self) -> None:
        if self.proc.returncode is not None:
            return
        self.proc.terminate()
        with anyio.move_on_after(self.settings.kill_grace_seconds):
            await self.proc.wait()
            return
        self.proc.kill()
        with anyio.move_on_after(self.settings.kill_grace_seconds):
            await self.proc.wait()
            return
        _log.warning("claude pid %s did not exit after SIGKILL", self.proc.pid)


async def run_claude(
    spec: RunSpec, settings: RunSettings, *, cancel: anyio.Event, on_event: EventSink
) -> RunOutcome:
    """Run claude for ``spec`` until it exits, times out, or ``cancel`` is set."""
    argv = build_argv(settings.claude_bin, spec)
    with spec.stderr_path.open("ab") as stderr:
        try:
            proc = await anyio.open_process(argv, cwd=spec.cwd, env=spec.env, stderr=stderr)
        except OSError as exc:
            return RunOutcome("failed", f"cannot start {settings.claude_bin}: {exc}")
        async with proc:
            supervisor = _Supervisor(proc, spec, settings, on_event)
            await supervisor.run(cancel)
    if supervisor.stop_reason == "timeout":
        return RunOutcome(
            "timeout", f"timeout: the job ran longer than max_job_seconds ({settings.max_seconds}s)"
        )
    if supervisor.stop_reason == "cancelled":
        return RunOutcome("cancelled", "cancelled by a caller")
    return outcome_from(supervisor.state, proc.returncode)
