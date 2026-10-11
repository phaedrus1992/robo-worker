import stat
from pathlib import Path

import pytest

from robo_worker.secret_files import (
    MIN_TOKEN_LENGTH,
    SecretError,
    load_callers,
    read_secret,
    write_private,
)


def _secret(path: Path, text: str, mode: int = 0o600) -> Path:
    path.write_text(text)
    path.chmod(mode)
    return path


def test_read_secret_strips_whitespace(tmp_path: Path) -> None:
    assert read_secret(_secret(tmp_path / "t", "sk-ant-oat01-x\n")) == "sk-ant-oat01-x"


def test_read_secret_refuses_group_readable(tmp_path: Path) -> None:
    path = _secret(tmp_path / "t", "x", 0o640)
    with pytest.raises(SecretError, match="chmod 600"):
        read_secret(path)


def test_read_secret_refuses_missing(tmp_path: Path) -> None:
    with pytest.raises(SecretError, match="cannot read"):
        read_secret(tmp_path / "missing")


def test_read_secret_refuses_empty(tmp_path: Path) -> None:
    with pytest.raises(SecretError, match="empty"):
        read_secret(_secret(tmp_path / "t", "  \n"))


def test_load_callers_maps_token_to_name(tmp_path: Path) -> None:
    agent, mac = "a" * MIN_TOKEN_LENGTH, "m" * MIN_TOKEN_LENGTH
    path = _secret(tmp_path / "c.toml", f'[callers]\nagent = "{agent}"\nmac = "{mac}"\n')
    assert load_callers(path) == {agent: "agent", mac: "mac"}


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("", "missing \\[callers\\]"),
        ("[callers]\n", "no callers"),
        ('[callers]\nagent = "short"\n', "at least"),
        ('[callers]\n"Bad Name" = "' + "x" * 40 + '"\n', "caller name"),
        ('[callers]\na = "' + "x" * 40 + '"\nb = "' + "x" * 40 + '"\n', "same token"),
        ("[callers]\nagent = 5\n", "must be a string"),
        ("not toml [", "not valid TOML"),
    ],
)
def test_load_callers_refuses_bad_files(tmp_path: Path, body: str, message: str) -> None:
    with pytest.raises(SecretError, match=message):
        load_callers(_secret(tmp_path / "c.toml", body))


def test_write_private_sets_mode_and_content(tmp_path: Path) -> None:
    path = tmp_path / "mcp.json"
    write_private(path, "{}")
    assert path.read_text() == "{}"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_write_private_resets_mode_of_stale_temp_file(tmp_path: Path) -> None:
    stale = tmp_path / ".mcp.json.tmp"
    stale.write_text("old")
    stale.chmod(0o644)
    path = tmp_path / "mcp.json"
    write_private(path, "{}")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
