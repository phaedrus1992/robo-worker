"""Queues, pauses, recovery, and the life of one job."""

import json
import logging
import math
import os
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Protocol

import anyio
from anyio.abc import TaskGroup

from robo_worker.checkout import CheckoutError, DirtyCheckout, is_dirty, prepare_checkout
from robo_worker.config import Config
from robo_worker.env import (
    EnvError,
    build_worker_env,
    check_bundles,
    check_no_api_key_helper,
    llmenv_export,
    validate_tags,
)
from robo_worker.repos import parse_repo
from robo_worker.runner import (
    CONTINUE_PROMPT,
    EventSink,
    RunOutcome,
    RunSettings,
    RunSpec,
    run_claude,
)
from robo_worker.secret_files import SecretError, read_secret, write_private
from robo_worker.states import TERMINAL, JobState
from robo_worker.store import Job, JobStore, StoreError, UnknownJob

_log = logging.getLogger(__name__)

# Progress text lands in an event file that callers poll. Keep each event small.
MAX_PHASE_CHARS = 64
MAX_DETAIL_CHARS = 2000

_STATE_FOR: dict[str, JobState] = {
    "done": JobState.DONE,
    "blocked": JobState.BLOCKED,
    "failed": JobState.FAILED,
    "timeout": JobState.FAILED,
    "auth": JobState.FAILED,
    "cancelled": JobState.CANCELLED,
    "rate_limited": JobState.RATE_LIMITED,
}


class SubmitError(ValueError):
    """A caller request is not valid in the current state."""


class RunFn(Protocol):
    async def __call__(
        self, spec: RunSpec, settings: RunSettings, *, cancel: anyio.Event, on_event: EventSink
    ) -> RunOutcome: ...


@dataclass
class Pauses:
    auth: str | None = None
    rate_limit_until: float | None = None
    repos: dict[str, str] = field(default_factory=dict)


class Scheduler:
    """One FIFO queue per repo, one running job per repo, and shared pauses."""

    def __init__(
        self,
        cfg: Config,
        store: JobStore,
        *,
        run: RunFn = run_claude,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._cfg = cfg
        self._store = store
        self._run = run
        self._clock = clock
        self._parent = dict(os.environ)
        self._settings = RunSettings.from_config(cfg)
        self.pauses = Pauses()
        self._busy: set[str] = set()
        self._cancels: dict[str, anyio.Event] = {}
        self._progress: dict[str, str] = {}
        self._wake = anyio.Event()
        # llmenv export pulls its config repo with no lock, so exports run one at a time.
        self._export_lock = anyio.Lock()

    @property
    def store(self) -> JobStore:
        return self._store

    @property
    def running_count(self) -> int:
        return len(self._busy)

    def kick(self) -> None:
        self._wake.set()

    # Caller requests.

    def submit(self, repo: str, prompt: str, tags: list[str] | None, caller: str) -> Job:
        name = parse_repo(repo)
        if not isinstance(prompt, str) or not prompt.strip():
            raise SubmitError("prompt is empty. Give the task for the worker.")
        chosen = validate_tags(list(self._cfg.default_tags) if tags is None else tags)
        denied = sorted(set(chosen) & set(self._cfg.deny_tags))
        if denied:
            raise SubmitError(f"tags {denied} are in the deny_tags config key")
        job = self._store.create(repo=name.slug, prompt=prompt, tags=chosen, caller=caller)
        self._store.append_event(job.id, "submitted", {"caller": caller, "tags": chosen})
        self.kick()
        return job

    def resume(self, job_id: str, answer: str) -> Job:
        job = self._store.get(job_id)
        if job.state is not JobState.BLOCKED:
            raise SubmitError(f"job {job_id} is {job.state}, not blocked")
        if not isinstance(answer, str) or not answer.strip():
            raise SubmitError("answer is empty. Give the answer to the blocked question.")
        job.resume_prompt = answer
        self._store.transition(job, JobState.QUEUED)
        self.kick()
        return job

    def cancel(self, job_id: str) -> Job:
        job = self._store.get(job_id)
        if job.state in TERMINAL:
            raise SubmitError(f"job {job_id} is already {job.state}")
        event = self._cancels.get(job_id)
        if event is not None:
            event.set()
            return job
        return self._store.transition(job, JobState.CANCELLED, "cancelled by a caller")

    def resume_queue(self, repo: str) -> None:
        slug = parse_repo(repo).slug
        if self.pauses.repos.pop(slug, None) is None:
            raise SubmitError(f"the queue for {slug} is not paused")
        self.kick()

    # Progress from the worker's own Claude session.

    def issue_progress_token(self, job_id: str) -> str:
        token = secrets.token_urlsafe(32)
        self._progress[token] = job_id
        return token

    def revoke_progress_token(self, token: str) -> None:
        self._progress.pop(token, None)

    def job_for_progress_token(self, token: str) -> str | None:
        return self._progress.get(token)

    def report_progress(self, job_id: str, phase: str, detail: str) -> None:
        if not phase.strip():
            raise SubmitError("phase is empty. Name the phase that starts now.")
        data = {"phase": phase[:MAX_PHASE_CHARS], "detail": detail[:MAX_DETAIL_CHARS]}
        self._store.append_event(job_id, "progress", data)

    # Lifecycle.

    def recover(self) -> None:
        """Requeue the jobs that a restart stopped, and restore the rate-limit pause."""
        for job in self._store.all(JobState.RUNNING):
            job.interruptions += 1
            self._store.transition(job, JobState.INTERRUPTED, "the service stopped during the job")
        for job in self._store.all(JobState.INTERRUPTED):
            if job.interruptions > self._cfg.max_interruptions:
                reason = f"interrupted {job.interruptions} times, more than max_interruptions"
                self._store.transition(job, JobState.FAILED, reason)
            else:
                self._store.transition(job, JobState.QUEUED)
        resets = [j.resets_at for j in self._store.all(JobState.RATE_LIMITED) if j.resets_at]
        if resets:
            self.pauses.rate_limit_until = max(resets)

    async def run_forever(self) -> None:
        async with anyio.create_task_group() as tg:
            while True:
                self._wake = anyio.Event()
                self._dispatch(tg)
                with anyio.move_on_after(self._seconds_to_next_check()):
                    await self._wake.wait()

    def _seconds_to_next_check(self) -> float:
        until = self.pauses.rate_limit_until
        return math.inf if until is None else max(0.0, until - self._clock())

    def _release_rate_limit(self) -> None:
        until = self.pauses.rate_limit_until
        if until is None or self._clock() < until:
            return
        self.pauses.rate_limit_until = None
        for job in self._store.all(JobState.RATE_LIMITED):
            self._store.transition(job, JobState.QUEUED)

    def _dispatch(self, tg: TaskGroup) -> None:
        self._release_rate_limit()
        if self.pauses.auth or self.pauses.rate_limit_until is not None:
            return
        for job in self._store.all(JobState.QUEUED):
            if len(self._busy) >= self._cfg.max_parallel_jobs:
                return
            if job.repo in self._busy or job.repo in self.pauses.repos:
                continue
            self._busy.add(job.repo)
            self._cancels[job.id] = anyio.Event()
            tg.start_soon(self._run_job, job.id, job.repo)

    async def _run_job(self, job_id: str, repo: str) -> None:
        try:
            await self._execute(self._store.get(job_id))
        except Exception as exc:
            # A bug in one job must not stop the queue for every other job.
            _log.exception("job %s failed with an internal error", job_id)
            self._settle(
                job_id,
                (JobState.QUEUED, JobState.RUNNING),
                JobState.FAILED,
                f"internal error: {exc!r}",
            )
        finally:
            cancel = self._cancels.pop(job_id, None)
            if cancel is not None and cancel.is_set():
                # A cancel that arrives while _prepare runs must not be lost when it stops early.
                self._settle(
                    job_id, (JobState.QUEUED,), JobState.CANCELLED, "cancelled by a caller"
                )
            self._busy.discard(repo)
            self.kick()

    def _settle(self, job_id: str, when: tuple[JobState, ...], new: JobState, reason: str) -> None:
        """Move the stored job to ``new`` if it is still in one of the ``when`` states."""
        try:
            current = self._store.get(job_id)
            if current.state in when:
                self._store.transition(current, new, reason)
        except (StoreError, UnknownJob) as exc:
            _log.error("cannot move job %s to %s: %s", job_id, new, exc)

    async def _execute(self, job: Job) -> None:
        prepared = await self._prepare(job)
        if prepared is None:
            return
        cwd, env = prepared
        outcome = await self._run_once(job, cwd, env)
        self._finish(job, outcome)
        await self._check_dirty_after(job, cwd)

    async def _prepare(self, job: Job) -> tuple[Path, dict[str, str]] | None:
        try:
            token = read_secret(self._cfg.oauth_token_file)
        except SecretError as exc:
            self.pauses.auth = str(exc)
            _log.warning("all queues paused for auth: %s", exc)
            self._store.append_event(job.id, "queue_paused", {"reason": f"auth: {exc}"})
            return None
        try:
            cwd = await prepare_checkout(
                self._cfg.git_bin,
                self._cfg.workspace_root,
                parse_repo(job.repo),
                self._cfg.clone_url_template,
                self._cfg.max_prepare_seconds,
            )
        except DirtyCheckout as exc:
            self._pause_repo(job, f"checkout_dirty: {exc}")
            return None
        except CheckoutError as exc:
            self._store.transition(job, JobState.FAILED, f"checkout failed: {exc}")
            return None
        try:
            return cwd, await self._worker_env(cwd, job.tags, token)
        except EnvError as exc:
            self._store.transition(job, JobState.FAILED, str(exc))
            return None

    async def _worker_env(self, cwd: Path, tags: list[str], token: str) -> dict[str, str]:
        async with self._export_lock:
            llmenv_vars = await llmenv_export(
                self._cfg.llmenv_bin, cwd, tags, self._parent, self._cfg.max_prepare_seconds
            )
        check_bundles(llmenv_vars, self._cfg.deny_bundles)
        check_no_api_key_helper(llmenv_vars)
        return build_worker_env(
            parent=self._parent,
            llmenv_vars=llmenv_vars,
            oauth_token=token,
            extra_env=self._cfg.extra_env,
        )

    async def _run_once(self, job: Job, cwd: Path, env: dict[str, str]) -> RunOutcome:
        cancel = self._cancels[job.id]
        if cancel.is_set():
            return RunOutcome("cancelled", "cancelled by a caller")
        token = self.issue_progress_token(job.id)
        try:
            spec = RunSpec(
                job_id=job.id,
                cwd=cwd,
                env=env,
                prompt=self._prompt_for(job),
                session_id=job.session_id,
                resume=job.started,
                mcp_config_path=self._write_mcp_config(job.id, token),
                stream_path=self._store.stream_path(job.id),
                stderr_path=self._store.stderr_path(job.id),
            )
            job.started = True
            job.resume_prompt = None
            self._store.transition(job, JobState.RUNNING)
            self._store.append_event(job.id, "started", {"resume": spec.resume})
            on_event = partial(self._store.append_event, job.id)
            return await self._run(spec, self._settings, cancel=cancel, on_event=on_event)
        finally:
            self.revoke_progress_token(token)

    @staticmethod
    def _prompt_for(job: Job) -> str:
        if not job.started:
            return job.prompt
        return job.resume_prompt or CONTINUE_PROMPT

    def _write_mcp_config(self, job_id: str, token: str) -> Path:
        server = {
            "type": "http",
            "url": f"{self._cfg.public_url}/mcp",
            "headers": {"Authorization": f"Bearer {token}"},
        }
        path = self._store.job_dir(job_id) / "mcp.json"
        write_private(path, json.dumps({"mcpServers": {"robo-worker": server}}))
        return path

    def _finish(self, job: Job, outcome: RunOutcome) -> None:
        job.result = outcome.result
        job.usage = outcome.usage
        if outcome.usage is not None:
            record = {"time": self._clock(), "kind": "job", "job_id": job.id}
            record |= {"caller": job.caller, "repo": job.repo, **outcome.usage}
            self._store.append_usage(record)
        if outcome.kind == "rate_limited":
            retry_at = self._clock() + self._cfg.rate_limit_retry_seconds
            job.resets_at = outcome.resets_at or retry_at
            self.pauses.rate_limit_until = job.resets_at
        elif outcome.kind == "auth":
            self.pauses.auth = outcome.reason
        self._store.transition(job, _STATE_FOR[outcome.kind], outcome.reason)
        self._store.append_event(
            job.id, "finished", {"state": job.state.value, "reason": outcome.reason}
        )

    def _pause_repo(self, job: Job, reason: str) -> None:
        self.pauses.repos[job.repo] = reason
        self._store.append_event(job.id, "queue_paused", {"reason": reason})

    async def _check_dirty_after(self, job: Job, cwd: Path) -> None:
        try:
            dirty = await is_dirty(self._cfg.git_bin, cwd, self._cfg.max_prepare_seconds)
        except CheckoutError as exc:
            self._pause_repo(job, f"checkout_unknown: {exc}")
            return
        if dirty:
            self._pause_repo(
                job,
                f"checkout_dirty: {cwd} has uncommitted changes. "
                f"Clean it, then call resume_queue('{job.repo}').",
            )
