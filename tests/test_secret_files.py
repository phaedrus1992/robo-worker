import os
import stat
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

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


@pytest.mark.parametrize("mode", [0o640, 0o440])
def test_read_secret_accepts_own_group_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: int
) -> None:
    # A k8s Secret volume gives root:<fsGroup> 0440 or 0640, and fsGroup is the service's group.
    path = _secret(tmp_path / "t", "x", mode)
    monkeypatch.setattr(os, "getegid", lambda: path.stat().st_gid)
    assert read_secret(path) == "x"


def test_read_secret_refuses_other_group_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _secret(tmp_path / "t", "x", 0o640)
    monkeypatch.setattr(os, "getegid", lambda: path.stat().st_gid + 1)
    with pytest.raises(SecretError, match="chmod 600"):
        read_secret(path)


@pytest.mark.parametrize("mode", [0o604, 0o644, 0o660, 0o602, 0o620])
def test_read_secret_refuses_others_and_group_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: int
) -> None:
    path = _secret(tmp_path / "t", "x", mode)
    monkeypatch.setattr(os, "getegid", lambda: path.stat().st_gid)
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


def test_write_private_removes_its_temp_file_on_failure(tmp_path: Path) -> None:
    target = tmp_path / "taken"
    target.mkdir()
    (target / "x").write_text("keep")
    with pytest.raises(OSError):
        write_private(target, "{}")
    assert sorted(p.name for p in tmp_path.iterdir()) == ["taken"]


def test_write_private_leaves_no_temp_file(tmp_path: Path) -> None:
    write_private(tmp_path / "a.json", "1")
    write_private(tmp_path / "a.json", "2")
    assert sorted(p.name for p in tmp_path.iterdir()) == ["a.json"]


def _allowed(mode: int, same_group: bool) -> bool:
    """The reference rule: no bit for others, and a group bit only as a read by our group."""
    if mode & 0o007:
        return False
    return not mode & 0o070 or (same_group and not mode & 0o030)


@settings(max_examples=150)
@given(st.integers(min_value=0, max_value=0o777), st.booleans())
def test_read_secret_follows_the_reference_rule(mode: int, same_group: bool) -> None:
    mode |= 0o400  # the owner can always read, so only the access rule decides
    with tempfile.TemporaryDirectory() as tmp:
        path = _secret(Path(tmp) / "t", "x", mode)
        gid = path.stat().st_gid
        with patch.object(os, "getegid", return_value=gid if same_group else gid + 1):
            if _allowed(mode, same_group):
                assert read_secret(path) == "x"
            else:
                with pytest.raises(SecretError, match="chmod 600"):
                    read_secret(path)
