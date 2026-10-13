"""Queues, pauses, recovery, and the life of one job."""

import json
import logging
import math
import os
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

import anyio
from anyio.abc import TaskGroup

from robo_worker.attempt import (
    AttemptSpec,
    PodFileError,
    PodPaths,
    StagingError,
    read_pod_events,
    read_pod_text,
    spec_from_json,
    stage_pod_dir,
)
from robo_worker.checkout import checkout_path
from robo_worker.config import Config
from robo_worker.env import validate_tags
from robo_worker.repos import parse_repo
from robo_worker.runner import CONTINUE_PROMPT, RunOutcome
from robo_worker.secret_files import SecretError, read_secret, write_private
from robo_worker.states import TERMINAL, JobState
from robo_worker.store import Job, JobStore, StoreError, UnknownJob

_log = logging.getLogger(__name__)

# The progress token lives in the job directory, so a restart can restore the mapping.
PROGRESS_TOKEN_NAME = "progress_token"
# The repo pauses live in state_root, so a restart keeps them.
REPO_PAUSES_NAME = "repo_pauses.json"

# Progress text lands in an event file that callers poll. Keep each event small.
MAX_PHASE_CHARS = 64
MAX_DETAIL_CHARS = 2000

_STATE_FOR: dict[str, JobState] = {
    "done": JobState.DONE,
    "blocked": JobState.BLOCKED,
    "failed": JobState.FAILED,
    "timeout": JobState.FAILED,
    "auth": JobState.FAILED,
    "prepare_failed": JobState.FAILED,
    "cancelled": JobState.CANCELLED,
    "rate_limited": JobState.RATE_LIMITED,
}
# Claude did not run in these attempts, so the job's session did not start or change.
_BEFORE_CLAUDE = frozenset({"prepare_failed", "checkout_paused"})


class SubmitError(ValueError):
    """A caller request is not valid in the current state."""


class LaunchFn(Protocol):
    """Run one attempt in a job pod and report its outcome. The production one is a k8s Job."""

    async def __call__(
        self, spec: AttemptSpec, token: str, *, cancel: anyio.Event
    ) -> RunOutcome: ...

    async def attach(self, job_id: str, attempt: int, *, cancel: anyio.Event) -> RunOutcome | None:
        """Wait for an attempt that an earlier control plane launched. None: no such run exists."""
        ...


@dataclass
class Pauses:
    auth: str | None = None
    rate_limit_until: float | None = None
    repos: dict[str, str] = field(default_factory=dict)


def _llmenv_master() -> Path:
    """The control plane's own llmenv config clone. Each attempt gets a copy of it."""
    override = os.environ.get("LLMENV_CONFIG_DIR")
    if override:
        return Path(override)
    return Path(os.environ.get("HOME", "/nonexistent")) / ".config" / "llmenv"


class Scheduler:
    """One FIFO queue per repo, one running job per repo, and shared pauses."""

    def __init__(
        self,
        cfg: Config,
        store: JobStore,
        *,
        launch: LaunchFn,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._cfg = cfg
        self._store = store
        self._launch = launch
        self._clock = clock
        self._llmenv_master = _llmenv_master()
        self.pauses = Pauses()
        self._busy: set[str] = set()
        self._cancels: dict[str, anyio.Event] = {}
        self._progress: dict[str, str] = {}
        self._restored: dict[str, str] = {}
        self._told_waiting: set[str] = set()
        self._wake = anyio.Event()

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
        self._save_repo_pauses()
        self.kick()

    # Progress from the worker's own Claude session.

    def issue_progress_token(self, job_id: str) -> str:
        token = secrets.token_urlsafe(32)
        self._progress[token] = job_id
        # A control-plane restart drops this mapping while the job pod still holds the token
        # in its mcp.json. The job directory carries it, so recover() reads it back.
        write_private(self._store.job_dir(job_id) / PROGRESS_TOKEN_NAME, token)
        return token

    def revoke_progress_token(self, token: str) -> None:
        self._progress.pop(token, None)

    def job_for_progress_token(self, token: str) -> str | None:
        return self._progress.get(token)

    def _restore_progress_token(self, job: Job) -> None:
        path = self._store.job_dir(job.id) / PROGRESS_TOKEN_NAME
        try:
            token = read_secret(path)
        except SecretError as exc:
            _log.warning("job %s: cannot restore its progress token: %s", job.id, exc)
            self._store.append_event(
                job.id, "progress_lost", {"reason": f"cannot restore the progress token: {exc}"}
            )
            return
        self._progress[token] = job.id
        self._restored[job.id] = token

    def report_progress(self, job_id: str, phase: str, detail: str) -> None:
        if not phase.strip():
            raise SubmitError("phase is empty. Name the phase that starts now.")
        data = {"phase": phase[:MAX_PHASE_CHARS], "detail": detail[:MAX_DETAIL_CHARS]}
        self._store.append_event(job_id, "progress", data)

    # Lifecycle.

    def recover(self) -> None:
        """Restore the pauses and the progress tokens, and requeue the interrupted jobs.

        A `running` job stays `running`: its pod can outlive this process. `run_forever`
        attaches to it before it starts anything new.
        """
        self.pauses.repos = self._load_repo_pauses()
        for job in self._store.all(JobState.RUNNING):
            # The job pod may still be alive. Its mcp.json holds a progress token that this
            # process never issued, so restore the mapping before anything else.
            self._restore_progress_token(job)
        for job in self._store.all(JobState.INTERRUPTED):
            self._requeue_or_fail(job)
        resets = [j.resets_at for j in self._store.all(JobState.RATE_LIMITED) if j.resets_at]
        if resets:
            self.pauses.rate_limit_until = max(resets)

    async def run_forever(self) -> None:
        async with anyio.create_task_group() as tg:
            self._reattach_running(tg)
            while True:
                self._wake = anyio.Event()
                self._dispatch(tg)
                with anyio.move_on_after(self._seconds_to_next_check()):
                    await self._wake.wait()

    def _seconds_to_next_check(self) -> float:
        until = self.pauses.rate_limit_until
        return math.inf if until is None else max(0.0, until - self._clock())

    def _reattach_running(self, tg: TaskGroup) -> None:
        for job in self._store.all(JobState.RUNNING):
            self._busy.add(job.repo)
            self._cancels[job.id] = anyio.Event()
            tg.start_soon(self._watch_attached, job.id, job.repo)

    async def _watch_attached(self, job_id: str, repo: str) -> None:
        try:
            job = self._store.get(job_id)
            cancel = self._cancels[job_id]
            outcome = await self._launch.attach(job_id, job.attempts, cancel=cancel)
            if outcome is None:
                outcome = RunOutcome("interrupted", "the job pod was gone after a service restart")
            self._finish(job, outcome, resumed=self._attempt_resumed(job))
        except Exception as exc:
            _log.exception("job %s failed with an internal error after a restart", job_id)
            self._settle(job_id, (JobState.RUNNING,), JobState.FAILED, f"internal error: {exc!r}")
        finally:
            token = self._restored.pop(job_id, None)
            if token is not None:
                self.revoke_progress_token(token)
            self._cancels.pop(job_id, None)
            self._busy.discard(repo)
            self.kick()

    def _attempt_resumed(self, job: Job) -> bool:
        """Whether the attempt that ran used --resume, from the attempt.json it was given."""
        path = PodPaths.for_job(self._cfg.state_root, job.id).spec
        try:
            return spec_from_json(read_pod_text(path)).resume
        except (OSError, PodFileError) as exc:
            _log.warning("job %s: cannot read attempt.json: %s", job.id, exc)
            # Keeping started=True is the safe side: a lost session fails loudly on --resume.
            return True

    def _release_rate_limit(self) -> None:
        until = self.pauses.rate_limit_until
        if until is None or self._clock() < until:
            return
        self.pauses.rate_limit_until = None
        for job in self._store.all(JobState.RATE_LIMITED):
            self._store.transition(job, JobState.QUEUED)

    def _repo_owners(self) -> dict[str, str]:
        """Map each repo to the started, unfinished job that owns its checkout.

        A blocked, rate-limited, or requeued job left its session and its work in progress in
        the checkout, and its resume expects to find both. No other job may start there.
        """
        owners: dict[str, str] = {}
        for state in (JobState.BLOCKED, JobState.RATE_LIMITED, JobState.QUEUED):
            for job in self._store.all(state):
                if job.started:
                    owners.setdefault(job.repo, job.id)
        return owners

    def _dispatch(self, tg: TaskGroup) -> None:
        self._release_rate_limit()
        if self.pauses.auth or self.pauses.rate_limit_until is not None:
            return
        owners = self._repo_owners()
        for job in self._store.all(JobState.QUEUED):
            if len(self._busy) >= self._cfg.max_parallel_jobs:
                return
            if job.repo in self._busy or job.repo in self.pauses.repos:
                continue
            owner = owners.get(job.repo, job.id)
            if owner != job.id:
                self._tell_waiting(job, owner)
                continue
            self._busy.add(job.repo)
            self._cancels[job.id] = anyio.Event()
            tg.start_soon(self._run_job, job.id, job.repo)

    def _tell_waiting(self, job: Job, owner: str) -> None:
        if job.id in self._told_waiting:
            return
        self._told_waiting.add(job.id)
        self._store.append_event(job.id, "waiting_for_owner", {"owner_job": owner})

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
                # A cancel that arrives before the launch must not be lost when it stops early.
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
        try:
            token = read_secret(self._cfg.oauth_token_file)
        except SecretError as exc:
            self.pauses.auth = str(exc)
            _log.warning("all queues paused for auth: %s", exc)
            self._store.append_event(job.id, "queue_paused", {"reason": f"auth: {exc}"})
            return
        resumed = job.started
        outcome = await self._run_once(job, token)
        if outcome is not None:
            self._finish(job, outcome, resumed=resumed)

    async def _run_once(self, job: Job, token: str) -> RunOutcome | None:
        cancel = self._cancels[job.id]
        if cancel.is_set():
            return RunOutcome("cancelled", "cancelled by a caller")
        paths = PodPaths.for_job(self._cfg.state_root, job.id)
        try:
            # Plain file work only: the control plane never runs a program on pod data.
            stage_pod_dir(paths, self._llmenv_master)
            checkout_path(self._cfg.workspace_root, parse_repo(job.repo)).mkdir(
                parents=True, exist_ok=True
            )
        except (OSError, StagingError) as exc:
            self._store.transition(job, JobState.FAILED, f"cannot stage the job pod: {exc}")
            return None
        progress = self.issue_progress_token(job.id)
        job.attempts += 1
        try:
            spec = AttemptSpec(
                job_id=job.id,
                attempt=job.attempts,
                repo=job.repo,
                tags=list(job.tags),
                prompt=self._prompt_for(job),
                session_id=job.session_id,
                resume=job.started,
            )
            self._write_mcp_config(paths, progress)
            job.started = True
            job.resume_prompt = None
            self._store.transition(job, JobState.RUNNING)
            self._store.append_event(
                job.id, "started", {"resume": spec.resume, "attempt": spec.attempt}
            )
            # The pod writes its own files under jobs/<id>/pod. This process appends nothing
            # for this job until the outcome comes back.
            return await self._launch(spec, token, cancel=cancel)
        finally:
            self.revoke_progress_token(progress)

    @staticmethod
    def _prompt_for(job: Job) -> str:
        if not job.started:
            return job.prompt
        return job.resume_prompt or CONTINUE_PROMPT

    def _write_mcp_config(self, paths: PodPaths, token: str) -> None:
        server = {
            "type": "http",
            "url": f"{self._cfg.public_url}/mcp",
            "headers": {"Authorization": f"Bearer {token}"},
        }
        write_private(paths.mcp_config, json.dumps({"mcpServers": {"robo-worker": server}}))

    def _finish(self, job: Job, outcome: RunOutcome, *, resumed: bool) -> None:
        paths = PodPaths.for_job(self._cfg.state_root, job.id)
        for kind, data in read_pod_events(paths.events):
            self._store.append_event(job.id, kind, data)
        paths.events.unlink(missing_ok=True)
        job.result = outcome.result
        job.usage = outcome.usage
        if outcome.usage is not None:
            record = {"time": self._clock(), "kind": "job", "job_id": job.id}
            record |= {"caller": job.caller, "repo": job.repo, **outcome.usage}
            self._store.append_usage(record)
        if outcome.kind in _BEFORE_CLAUDE:
            job.started = resumed
        if outcome.kind == "interrupted":
            job.interruptions += 1
            self._store.transition(job, JobState.INTERRUPTED, outcome.reason)
            self._requeue_or_fail(job)
        elif outcome.kind == "checkout_paused":
            self._pause_repo(job, outcome.reason)
            # The pause is not the job's fault, so it does not count as an interruption.
            self._store.transition(job, JobState.INTERRUPTED, outcome.reason)
            self._store.transition(job, JobState.QUEUED)
        else:
            self._apply_ending(job, outcome)
        self._store.append_event(
            job.id, "finished", {"state": job.state.value, "reason": outcome.reason}
        )

    def _apply_ending(self, job: Job, outcome: RunOutcome) -> None:
        if outcome.kind == "rate_limited":
            retry_at = self._clock() + self._cfg.rate_limit_retry_seconds
            job.resets_at = outcome.resets_at or retry_at
            self.pauses.rate_limit_until = job.resets_at
        elif outcome.kind == "auth":
            self.pauses.auth = outcome.reason
        self._store.transition(job, _STATE_FOR[outcome.kind], outcome.reason)
        if job.state in TERMINAL and outcome.checkout_problem:
            self._pause_repo(
                job,
                f"{outcome.checkout_problem}. Clean it, then call resume_queue('{job.repo}').",
            )

    def _requeue_or_fail(self, job: Job) -> None:
        """Move an interrupted job back to the queue, or fail it past max_interruptions."""
        if job.interruptions > self._cfg.max_interruptions:
            limit = f"interrupted {job.interruptions} times, more than max_interruptions"
            self._store.transition(job, JobState.FAILED, limit)
        else:
            self._store.transition(job, JobState.QUEUED)

    def _pause_repo(self, job: Job, reason: str) -> None:
        self.pauses.repos[job.repo] = reason
        self._save_repo_pauses()
        self._store.append_event(job.id, "queue_paused", {"reason": reason})

    def _save_repo_pauses(self) -> None:
        write_private(self._cfg.state_root / REPO_PAUSES_NAME, json.dumps(self.pauses.repos))

    def _load_repo_pauses(self) -> dict[str, str]:
        path = self._cfg.state_root / REPO_PAUSES_NAME
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as exc:
            _log.error("cannot read %s, so no repo starts paused: %s", path, exc)
            return {}
        if not isinstance(data, dict):
            return {}
        return {k: v for k, v in data.items() if isinstance(k, str) and isinstance(v, str)}
