from pathlib import Path

import pytest

from robo_worker.cli import main
from robo_worker.job_main import EXIT_BAD_INPUT

EXAMPLE = Path(__file__).parent.parent / "config.example.toml"


def test_missing_config_exits_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["serve", "--config", str(tmp_path / "nope.toml")]) == 2
    assert "cannot read config" in capsys.readouterr().err


def test_job_mode_with_a_missing_config_exits_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    argv = ["job", "--config", str(tmp_path / "nope.toml"), "--job-id", "a" * 32, "--attempt", "1"]
    assert main(argv) == 2
    assert "cannot read config" in capsys.readouterr().err


def test_missing_callers_file_exits_2(capsys: pytest.CaptureFixture[str]) -> None:
    # The example config points at /etc/robo-worker/secrets/callers.toml, absent here.
    assert main(["serve", "--config", str(EXAMPLE)]) == 2
    assert "callers.toml" in capsys.readouterr().err


def test_job_mode_needs_no_callers_file(tmp_path: Path) -> None:
    # A job pod reads no callers file. This job has no run spec, so it records nothing.
    config = tmp_path / "config.toml"
    config.write_text(EXAMPLE.read_text().replace('"/state"', f'"{tmp_path / "state"}"'))
    argv = ["job", "--config", str(config), "--job-id", "a" * 32, "--attempt", "1"]
    assert main(argv) == EXIT_BAD_INPUT


def test_config_flag_is_required() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["serve"])
    assert exc.value.code == 2


def test_a_subcommand_is_required() -> None:
    with pytest.raises(SystemExit) as exc:
        main([])
    assert exc.value.code == 2


def test_job_mode_needs_a_job_id(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["job", "--config", str(tmp_path / "c.toml")])
    assert exc.value.code == 2


@pytest.mark.parametrize("attempt", ["0", "-1", "x", "1.5", "\u0661"])
def test_job_mode_needs_a_positive_attempt(tmp_path: Path, attempt: str) -> None:
    argv = ["job", "--config", str(tmp_path / "c.toml"), "--job-id", "a" * 32, "--attempt", attempt]
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == 2
