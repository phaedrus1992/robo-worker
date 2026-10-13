"""The job-pod entrypoint: run one attempt of a job, record its outcome, exit.

Exit 0 means the pod recorded an outcome, whatever that outcome was.
A non-zero exit means no outcome was recorded, and the control plane reads the failed Job
as `interrupted`.
"""

import logging
import os
import signal
from pathlib import Path

import anyio

from robo_worker.attempt import (
    AttemptSpec,
    PodFileError,
    PodPaths,
    outcome_to_json,
    read_pod_text,
    run_attempt,
    spec_from_json,
)
from robo_worker.config import Config
from robo_worker.runner import RunOutcome
from robo_worker.secret_files import SecretError, read_secret, write_private
from robo_worker.store import JobStore, UnknownJob

_log = logging.getLogger(__name__)

# The per-attempt token Secret is mounted here.
TOKEN_FILE = Path("/run/robo-worker/token/claude-oauth-token")
# Exit code for "no outcome recorded". It makes the Job fail.
EXIT_BAD_INPUT = 3
SIGTERM_REASON = "the job pod got SIGTERM (eviction, node drain, or Job delete)"


async def _run(cfg: Config, spec: AttemptSpec, token: str) -> RunOutcome:
    cancel = anyio.Event()
    stopped_by_signal = False
    async with anyio.create_task_group() as tg:
        with anyio.open_signal_receiver(signal.SIGTERM) as signals:

            async def stop_on_signal() -> None:
                nonlocal stopped_by_signal
                async for _ in signals:
                    _log.warning("job %s: SIGTERM, stopping claude", spec.job_id)
                    stopped_by_signal = True
                    cancel.set()
                    return

            tg.start_soon(stop_on_signal)
            outcome = await run_attempt(cfg, spec, token, parent=dict(os.environ), cancel=cancel)
            tg.cancel_scope.cancel()
    if stopped_by_signal:
        # Nobody asked for this stop. A caller cancel deletes the Job and records "cancelled"
        # in the control plane, so the pod reports a stop it did not choose as an interruption.
        return RunOutcome("interrupted", SIGTERM_REASON, usage=outcome.usage)
    return outcome


def _load(cfg: Config, job_id: str, attempt: int, token_file: Path) -> tuple[AttemptSpec, str]:
    JobStore(cfg.state_root).job_dir(job_id)  # checks the shape of job_id
    paths = PodPaths.for_job(cfg.state_root, job_id)
    spec = spec_from_json(read_pod_text(paths.spec))
    if (spec.job_id, spec.attempt) != (job_id, attempt):
        raise PodFileError(
            f"attempt.json is for job {spec.job_id} attempt {spec.attempt}, "
            f"not job {job_id} attempt {attempt}"
        )
    return spec, read_secret(token_file)


def job_main(cfg: Config, job_id: str, attempt: int, *, token_file: Path = TOKEN_FILE) -> int:
    """Run attempt ``attempt`` of job ``job_id`` in this pod. Return 0 when it recorded one."""
    try:
        spec, token = _load(cfg, job_id, attempt, token_file)
    except (OSError, PodFileError, SecretError, UnknownJob) as exc:
        _log.error("job %s attempt %d: cannot read its input: %s", job_id, attempt, exc)
        return EXIT_BAD_INPUT
    try:
        outcome = anyio.run(_run, cfg, spec, token)
    except Exception as exc:
        # A bug here repeats on each resume, so it ends the job with its cause.
        _log.exception("job %s attempt %d: internal error", job_id, attempt)
        outcome = RunOutcome("failed", f"internal error in the job pod: {exc!r}")
    paths = PodPaths.for_job(cfg.state_root, job_id)
    write_private(paths.outcome(attempt), outcome_to_json(outcome))
    _log.info("job %s attempt %d: %s: %s", job_id, attempt, outcome.kind, outcome.reason)
    return 0
