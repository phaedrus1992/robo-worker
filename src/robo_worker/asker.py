"""The ask call: one text-only claude -p turn, with no tools, repo, or queue."""

import logging
import os
import time
from dataclasses import dataclass
from typing import Any

import anyio

from robo_worker.config import Config
from robo_worker.env import build_worker_env
from robo_worker.runner import RunSettings, StreamState, apply_line, usage_of
from robo_worker.scheduler import Pauses
from robo_worker.secret_files import SecretError, read_secret
from robo_worker.store import JobStore

_log = logging.getLogger(__name__)
_HTTP_AUTH = frozenset({401, 403})


class AskError(RuntimeError):
    """The ask call did not give a text answer."""


@dataclass(frozen=True)
class AskResult:
    text: str
    usage: dict[str, Any]


def _log_event(kind: str, data: dict[str, Any]) -> None:
    # An ask has no event file, so a bad stream line goes to the service log.
    if kind == "stream_error":
        _log.warning("ask: %s: %s", kind, data)


class Asker:
    def __init__(self, cfg: Config, store: JobStore, pauses: Pauses) -> None:
        self._cfg = cfg
        self._store = store
        self._pauses = pauses
        self._parent = dict(os.environ)
        self._settings = RunSettings.from_config(cfg)
        self._limit = anyio.Semaphore(cfg.max_parallel_asks)
        self._cwd = cfg.state_root / "ask"
        self._cwd.mkdir(parents=True, exist_ok=True)

    def argv(self, system: str | None) -> list[str]:
        prompt_flags = ["--system-prompt", system] if system else []
        return [
            self._cfg.claude_bin,
            "-p",
            "--output-format",
            "stream-json",
            "--verbose",
            "--no-session-persistence",
            "--strict-mcp-config",
            *prompt_flags,
            # --tools takes several values; "" turns off every tool. Nothing may follow it.
            "--tools",
            "",
        ]

    async def ask(self, prompt: str, system: str | None, caller: str) -> AskResult:
        if not isinstance(prompt, str) or not prompt.strip():
            raise AskError("prompt is empty. Give the question.")
        if self._pauses.auth:
            raise AskError(f"asks are paused for auth: {self._pauses.auth}")
        try:
            token = read_secret(self._cfg.oauth_token_file)
        except SecretError as exc:
            raise AskError(str(exc)) from exc
        env = build_worker_env(
            parent=self._parent, llmenv_vars={}, oauth_token=token, extra_env=self._cfg.extra_env
        )
        async with self._limit:
            try:
                with anyio.fail_after(self._cfg.max_ask_seconds):
                    done = await anyio.run_process(
                        self.argv(system),
                        input=prompt.encode("utf-8"),
                        cwd=self._cwd,
                        env=env,
                        check=False,
                    )
            except TimeoutError:
                raise AskError("ask ran longer than the max_ask_seconds config key") from None
        return self._answer(done.stdout, done.returncode, caller)

    def _answer(self, stdout: bytes, code: int, caller: str) -> AskResult:
        state = StreamState()
        for line in stdout.decode("utf-8", errors="replace").splitlines():
            if line.strip():
                apply_line(line, state, self._settings, _log_event)
        if state.violation:
            raise AskError(state.violation)
        result = state.result
        if result is None:
            if state.limited:
                raise AskError(f"the plan rate limit is reached until {state.limited.resets_at}")
            raise AskError(f"claude exited with code {code} and gave no result")
        if result.is_error or result.subtype != "success" or result.text is None:
            reason = f"claude ended with {result.subtype!r} (HTTP {result.api_error_status})"
            if result.api_error_status in _HTTP_AUTH:
                self._pauses.auth = reason
            raise AskError(reason)
        usage = usage_of(result)
        self._store.append_usage({"time": time.time(), "kind": "ask", "caller": caller, **usage})
        return AskResult(text=result.text, usage=usage)
