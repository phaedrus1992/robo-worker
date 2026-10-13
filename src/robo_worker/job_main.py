"""The job-pod entrypoint: run one task, record its outcome, exit.

Exit 0 means the pod recorded an outcome, whatever that outcome was.
A non-zero exit means no outcome was recorded, and the control plane reads the failed Job
as `interrupted`.
"""

import json
import logging
import signal
from dataclasses import fields
from functools import partial
from pathlib import Path

import anyio

from robo_worker.config import Config
from robo_worker.launcher import OUTCOME_NAME, RUN_SPEC_NAME, outcome_to_json
from robo_worker.runner import RunOutcome, RunSettings, RunSpec, run_claude
from robo_worker.secret_files import write_private
from robo_worker.store import JobStore, UnknownJob

_log = logging.getLogger(__name__)

# The per-job env Secret is mounted here, one file per variable. It is a volume and not
# envFrom, so the image's own ENV lines never join the allowlist.
ENV_DIR = Path("/run/robo-worker/env")
# Exit codes for "no outcome recorded". Each one makes the Job fail.
EXIT_BAD_INPUT = 3
_PATH_FIELDS = ("cwd", "mcp_config_path", "stream_path", "stderr_path")
_SPEC_FIELDS = frozenset(f.name for f in fields(RunSpec)) - {"env"}


def load_env(env_dir: Path) -> dict[str, str]:
    """Read the allowlisted worker environment from the mounted Secret."""
    # A Secret volume also holds ..data and ..<timestamp> entries, which start with a dot.
    files = [p for p in env_dir.iterdir() if not p.name.startswith(".")]
    if not files:
        raise ValueError(f"{env_dir} holds no variables. Check the env volume of the job pod.")
    return {p.name: p.read_text(encoding="utf-8") for p in files}


def load_run_spec(path: Path, env: dict[str, str]) -> RunSpec:
    """Rebuild the RunSpec that the control plane wrote, with the env from the Secret."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or set(raw) != _SPEC_FIELDS:
        raise ValueError(f"{path} must hold exactly the RunSpec fields except env")
    for name in _PATH_FIELDS:
        raw[name] = Path(raw[name])
    return RunSpec(**raw, env=env)


async def _run(spec: RunSpec, settings: RunSettings, store: JobStore) -> RunOutcome:
    cancel = anyio.Event()
    on_event = partial(store.append_event, spec.job_id)
    async with anyio.create_task_group() as tg:
        # k8s stops a deleted or evicted pod with SIGTERM. The runner then stops claude with
        # its own SIGTERM and SIGKILL path, and the pod still records an outcome.
        with anyio.open_signal_receiver(signal.SIGTERM) as signals:

            async def stop_on_signal() -> None:
                async for _ in signals:
                    _log.warning("job %s: SIGTERM, stopping claude", spec.job_id)
                    cancel.set()
                    return

            tg.start_soon(stop_on_signal)
            outcome = await run_claude(spec, settings, cancel=cancel, on_event=on_event)
            tg.cancel_scope.cancel()
    return outcome


def job_main(cfg: Config, job_id: str, *, env_dir: Path = ENV_DIR) -> int:
    """Run the job's current attempt in this pod. Return 0 when an outcome was recorded."""
    store = JobStore(cfg.state_root)
    try:
        job_dir = store.job_dir(job_id)
        spec = load_run_spec(job_dir / RUN_SPEC_NAME, load_env(env_dir))
    except (OSError, ValueError, TypeError, UnknownJob) as exc:
        _log.error("job %s: cannot read the run spec or the env: %s", job_id, exc)
        return EXIT_BAD_INPUT
    if spec.job_id != job_id:
        _log.error("job %s: run_spec.json names job %s", job_id, spec.job_id)
        return EXIT_BAD_INPUT
    outcome = anyio.run(_run, spec, RunSettings.from_config(cfg), store)
    write_private(job_dir / OUTCOME_NAME, outcome_to_json(outcome))
    _log.info("job %s attempt %d: %s: %s", job_id, spec.attempt, outcome.kind, outcome.reason)
    return 0
