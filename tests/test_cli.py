from pathlib import Path

import pytest

from robo_worker.cli import main

EXAMPLE = Path(__file__).parent.parent / "config.example.toml"


def test_missing_config_exits_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--config", str(tmp_path / "nope.toml")]) == 2
    assert "cannot read config" in capsys.readouterr().err


def test_missing_callers_file_exits_2(capsys: pytest.CaptureFixture[str]) -> None:
    # The example config points at /home/robo-worker/secrets/callers.toml, absent here.
    assert main(["--config", str(EXAMPLE)]) == 2
    assert "callers.toml" in capsys.readouterr().err


def test_config_flag_is_required(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main([])
    assert exc.value.code == 2
