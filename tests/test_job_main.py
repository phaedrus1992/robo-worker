import json
import os
import signal
import threading
import time
from dataclasses import asdict
from pathlib import Path

import pytest

from robo_worker.config import Config
from robo_worker.job_main import EXIT_BAD_INPUT, job_main, load_env
from robo_worker.launcher import OUTCOME_NAME, RUN_SPEC_NAME, outcome_from_json
from robo_worker.runner import RunSpec
from robo_worker.store import JobStore
from tests.conftest import FIXTURES

JOB_ID = "c" * 32


def _write_run_spec(store: JobStore, tmp_path: Path, **fields: object) -> Path:
    job_dir = store.job_dir(JOB_ID)
    job_dir.mkdir(parents=True)
    mcp = job_dir / "mcp.json"
    mcp.write_text("{}")
    spec = RunSpec(
        job_id=JOB_ID,
        cwd=tmp_path,
        env={},
        prompt="do it",
        session_id="d" * 36,
        resume=False,
        mcp_config_path=mcp,
        stream_path=job_dir / "stream.jsonl",
        stderr_path=job_dir / "stderr.log",
        attempt=1,
    )
    # The same shape as K8sLauncher writes: every field except env, paths as strings.
    data = {k: v for k, v in asdict(spec).items() if k != "env"} | fields
    (job_dir / RUN_SPEC_NAME).write_text(json.dumps(data, default=str))
    return job_dir


def _env_dir(tmp_path: Path, **variables: str) -> Path:
    env_dir = tmp_path / "env"
    env_dir.mkdir()
    # A real Secret volume holds these dot entries beside the variable files.
    (env_dir / "..data").mkdir()
    for name, value in variables.items():
        (env_dir / name).write_text(value)
    return env_dir


def _worker_env(tmp_path: Path, **extra: str) -> dict[str, str]:
    return {
        "PATH": os.environ["PATH"],
        "FAKE_CLAUDE_FIXTURES": str(FIXTURES / "success.jsonl"),
        "FAKE_CLAUDE_RECORD": str(tmp_path / "record.jsonl"),
        **extra,
    }


def _recorded_env(tmp_path: Path) -> set[str]:
    lines = (tmp_path / "record.jsonl").read_text().splitlines()
    return set(json.loads(lines[-1])["env"])


def test_job_main_runs_with_exactly_the_secret_env(
    config: Config, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-or-v1-must-not-leak")
    store = JobStore(config.state_root)
    job_dir = _write_run_spec(store, tmp_path)
    variables = _worker_env(tmp_path)

    assert job_main(config, JOB_ID, env_dir=_env_dir(tmp_path, **variables)) == 0

    outcome = outcome_from_json((job_dir / OUTCOME_NAME).read_text())
    assert outcome.kind == "done"
    seen = _recorded_env(tmp_path)
    assert set(variables) <= seen
    assert "ANTHROPIC_API_KEY" not in seen
    assert not any(name.startswith(".") for name in seen)
    kinds = [e["kind"] for e in store.events(JOB_ID, 0)[0]]
    assert kinds  # the pod appends the run's events to the shared store


def test_load_env_skips_the_secret_volume_dot_entries(tmp_path: Path) -> None:
    assert load_env(_env_dir(tmp_path, A="1", B="two words\n")) == {"A": "1", "B": "two words\n"}


def test_an_empty_env_dir_records_nothing(config: Config, tmp_path: Path) -> None:
    job_dir = _write_run_spec(JobStore(config.state_root), tmp_path)

    assert job_main(config, JOB_ID, env_dir=_env_dir(tmp_path)) == EXIT_BAD_INPUT
    assert not (job_dir / OUTCOME_NAME).exists()


def test_a_missing_run_spec_records_nothing(config: Config, tmp_path: Path) -> None:
    job_dir = JobStore(config.state_root).job_dir(JOB_ID)
    job_dir.mkdir(parents=True)
    env_dir = _env_dir(tmp_path, **_worker_env(tmp_path))

    assert job_main(config, JOB_ID, env_dir=env_dir) == EXIT_BAD_INPUT
    assert not (job_dir / OUTCOME_NAME).exists()


@pytest.mark.parametrize("fields", [{"env": {"A": "1"}}, {"surprise": 1}, {"job_id": "e" * 32}])
def test_a_bad_run_spec_records_nothing(
    config: Config, tmp_path: Path, fields: dict[str, object]
) -> None:
    job_dir = _write_run_spec(JobStore(config.state_root), tmp_path, **fields)
    env_dir = _env_dir(tmp_path, **_worker_env(tmp_path))

    assert job_main(config, JOB_ID, env_dir=env_dir) == EXIT_BAD_INPUT
    assert not (job_dir / OUTCOME_NAME).exists()


def test_a_bad_job_id_records_nothing(config: Config, tmp_path: Path) -> None:
    env_dir = _env_dir(tmp_path, **_worker_env(tmp_path))
    assert job_main(config, "../escape", env_dir=env_dir) == EXIT_BAD_INPUT


def test_sigterm_stops_claude_and_records_cancelled(config: Config, tmp_path: Path) -> None:
    store = JobStore(config.state_root)
    job_dir = _write_run_spec(store, tmp_path)
    pidfile = tmp_path / "claude.pid"
    variables = _worker_env(tmp_path, FAKE_CLAUDE_SLEEP="30", FAKE_CLAUDE_PIDFILE=str(pidfile))

    def terminate_when_claude_runs() -> None:
        deadline = time.monotonic() + 10
        while not pidfile.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        # The pid file exists only after job_main opened its SIGTERM receiver.
        os.kill(os.getpid(), signal.SIGTERM)

    killer = threading.Thread(target=terminate_when_claude_runs)
    killer.start()
    started = time.monotonic()
    code = job_main(config, JOB_ID, env_dir=_env_dir(tmp_path, **variables))
    killer.join()

    assert code == 0
    assert time.monotonic() - started < 15
    assert outcome_from_json((job_dir / OUTCOME_NAME).read_text()).kind == "cancelled"
