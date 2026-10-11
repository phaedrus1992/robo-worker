import re
from datetime import date
from pathlib import Path

import pytest

from robo_worker.config import ConfigError, load_config

EXAMPLE = Path(__file__).parent.parent / "config.example.toml"


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(text)
    return path


def _example_with(tmp_path: Path, old: str, new: str) -> Path:
    text = EXAMPLE.read_text()
    assert old in text, old
    return _write(tmp_path, text.replace(old, new, 1))


def test_example_config_loads() -> None:
    cfg = load_config(EXAMPLE)
    assert cfg.listen_port == 8765
    assert cfg.public_url == "http://192.168.1.20:8765"
    assert cfg.oauth_token_created == date(2026, 10, 12)
    assert cfg.deny_output_styles == ("ben",)
    assert cfg.workspace_root == Path("/home/robo-worker/work")


def test_public_url_trailing_slash_is_removed(tmp_path: Path) -> None:
    path = _example_with(tmp_path, ':8765"\n# Host', ':8765/"\n# Host')
    assert load_config(path).public_url == "http://192.168.1.20:8765"


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ("[limits]", "[limitz]", "missing [limits] table"),
        ("max_job_seconds = 14400\n", "", "missing limits.max_job_seconds"),
        ("max_prepare_seconds = 600", "max_prepare_seconds = 0", "greater than 0"),
        ("max_job_seconds = 14400", "max_job_seconds = 0", "greater than 0"),
        ("max_job_seconds = 14400", 'max_job_seconds = "1"', "must be int"),
        ("max_job_seconds = 14400", "max_job_seconds = true", "must be int"),
        ("listen_port = 8765", "listen_port = 70000", "between 1 and 65535"),
        ('"/home/robo-worker/work"', '"work"', "absolute path"),
        ("deny_tags = []", 'deny_tags = [""]', "non-empty strings"),
        ("{owner}/{name}.git", "{owner}.git", "must contain"),
        ("{owner}/{name}.git", "{owner}/{name}/{x}.git", "only {owner} and {name}"),
        (
            "oauth_token_created = 2026-10-12",
            "oauth_token_created = 2026-10-12T09:00:00",
            "a date without a time",
        ),
    ],
)
def test_bad_values_name_the_key(tmp_path: Path, old: str, new: str, message: str) -> None:
    with pytest.raises(ConfigError, match=re.escape(message)):
        load_config(_example_with(tmp_path, old, new))


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read config"):
        load_config(tmp_path / "nope.toml")


def test_invalid_toml(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not valid TOML"):
        load_config(_write(tmp_path, "[server"))
