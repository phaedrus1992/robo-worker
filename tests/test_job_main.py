import json
import os
import signal
import threading
import time
from pathlib import Path

import pytest

from robo_worker.attempt import AttemptSpec, PodPaths, outcome_from_json, spec_to_json
from robo_worker.config import Config
from robo_worker.job_main import EXIT_BAD_INPUT, SIGTERM_REASON, job_main
from tests.conftest import FIXTURES, make_origin, write_secret

pytestmark = pytest.mark.usefixtures("isolated_git")

JOB_ID = "c" * 32


def _stage(config: Config, tmp_path: Path, **changes: object) -> PodPaths:
    paths = PodPaths.for_job(config.state_root, JOB_ID)
    paths.root.mkdir(parents=True)
    paths.mcp_config.write_text("{}")
    spec = AttemptSpec(
        job_id=JOB_ID,
        attempt=1,
        repo="o/r",
        tags=[],
        prompt="do it",
        session_id="d" * 36,
        resume=False,
    )
    data = json.loads(spec_to_json(spec)) | changes
    paths.spec.write_text(json.dumps(data))
    return paths


@pytest.fixture
def token_file(tmp_path: Path) -> Path:
    return write_secret(tmp_path / "token", "sk-ant-oat01-test\n")


@pytest.fixture
def fake_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("FAKE_CLAUDE_FIXTURES", str(FIXTURES / "success.jsonl"))
    record = tmp_path / "record.jsonl"
    monkeypatch.setenv("FAKE_CLAUDE_RECORD", str(record))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-or-v1-must-not-leak")
    return record


def _outcome(paths: PodPaths, attempt: int = 1) -> str:
    return outcome_from_json(paths.outcome(attempt).read_text()).kind


def test_job_main_prepares_and_runs_in_the_pod(
    config: Config, tmp_path: Path, token_file: Path, fake_env: Path
) -> None:
    make_origin(tmp_path)
    paths = _stage(config, tmp_path)

    assert job_main(config, JOB_ID, 1, token_file=token_file) == 0

    assert _outcome(paths) == "done"
    call = json.loads(fake_env.read_text().splitlines()[-1])
    assert call["cwd"] == str(config.workspace_root / "o" / "r")
    assert "CLAUDE_CODE_OAUTH_TOKEN" in call["env"]
    assert "ANTHROPIC_API_KEY" not in call["env"]


def test_a_dirty_checkout_before_the_run_pauses_and_does_not_start_claude(
    config: Config, tmp_path: Path, token_file: Path, fake_env: Path
) -> None:
    make_origin(tmp_path)
    paths = _stage(config, tmp_path)
    assert job_main(config, JOB_ID, 1, token_file=token_file) == 0
    (config.workspace_root / "o" / "r" / "scratch.txt").write_text("wip\n")
    paths.spec.write_text(paths.spec.read_text().replace('"attempt": 1', '"attempt": 2'))

    assert job_main(config, JOB_ID, 2, token_file=token_file) == 0

    outcome = outcome_from_json(paths.outcome(2).read_text())
    assert outcome.kind == "checkout_paused"
    assert outcome.reason.startswith("checkout_dirty")
    assert len(fake_env.read_text().splitlines()) == 1


def test_a_resume_with_a_missing_checkout_fails_and_does_not_clone(
    config: Config, tmp_path: Path, token_file: Path, fake_env: Path
) -> None:
    make_origin(tmp_path)
    paths = _stage(config, tmp_path, resume=True)

    assert job_main(config, JOB_ID, 1, token_file=token_file) == 0

    outcome = outcome_from_json(paths.outcome(1).read_text())
    assert outcome.kind == "prepare_failed"
    assert "cannot resume" in outcome.reason
    assert not (config.workspace_root / "o" / "r").exists()


@pytest.mark.parametrize(
    "changes", [{"surprise": 1}, {"job_id": "e" * 32}, {"attempt": 2}, {"attempt": True}]
)
def test_a_bad_attempt_file_records_nothing(
    config: Config, tmp_path: Path, token_file: Path, changes: dict[str, object]
) -> None:
    paths = _stage(config, tmp_path, **changes)

    assert job_main(config, JOB_ID, 1, token_file=token_file) == EXIT_BAD_INPUT
    assert not paths.outcome(1).exists()


def test_a_missing_token_records_nothing(config: Config, tmp_path: Path) -> None:
    paths = _stage(config, tmp_path)

    assert job_main(config, JOB_ID, 1, token_file=tmp_path / "missing") == EXIT_BAD_INPUT
    assert not paths.outcome(1).exists()


def test_a_planted_link_as_attempt_file_records_nothing(
    config: Config, tmp_path: Path, token_file: Path
) -> None:
    paths = _stage(config, tmp_path)
    real = tmp_path / "elsewhere.json"
    real.write_text(paths.spec.read_text())
    paths.spec.unlink()
    paths.spec.symlink_to(real)

    assert job_main(config, JOB_ID, 1, token_file=token_file) == EXIT_BAD_INPUT


def test_a_bad_job_id_records_nothing(config: Config, token_file: Path) -> None:
    assert job_main(config, "../escape", 1, token_file=token_file) == EXIT_BAD_INPUT


def test_an_internal_error_records_a_failed_outcome(
    config: Config, tmp_path: Path, token_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _stage(config, tmp_path)

    async def broken(*args: object, **kwargs: object) -> None:
        raise RuntimeError("bug in the pod")

    monkeypatch.setattr("robo_worker.job_main.run_attempt", broken)

    assert job_main(config, JOB_ID, 1, token_file=token_file) == 0
    outcome = outcome_from_json(paths.outcome(1).read_text())
    assert outcome.kind == "failed"
    assert "bug in the pod" in outcome.reason


def test_sigterm_stops_claude_and_records_interrupted(
    config: Config,
    tmp_path: Path,
    token_file: Path,
    fake_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    make_origin(tmp_path)
    paths = _stage(config, tmp_path)
    pidfile = tmp_path / "claude.pid"
    monkeypatch.setenv("FAKE_CLAUDE_SLEEP", "30")
    monkeypatch.setenv("FAKE_CLAUDE_PIDFILE", str(pidfile))

    def terminate_when_claude_runs() -> None:
        deadline = time.monotonic() + 15
        while not pidfile.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        # The pid file exists only after job_main opened its SIGTERM receiver.
        os.kill(os.getpid(), signal.SIGTERM)

    killer = threading.Thread(target=terminate_when_claude_runs)
    killer.start()
    started = time.monotonic()
    code = job_main(config, JOB_ID, 1, token_file=token_file)
    killer.join()

    assert code == 0
    assert time.monotonic() - started < 20
    outcome = outcome_from_json(paths.outcome(1).read_text())
    # Nobody asked for this stop, so the job must come back, not end as cancelled.
    assert outcome.kind == "interrupted"
    assert outcome.reason == SIGTERM_REASON
