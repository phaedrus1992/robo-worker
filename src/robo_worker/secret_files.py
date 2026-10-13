"""Read secret files that only this user or group can read, and write owner-only files."""

import contextlib
import os
import re
import stat
import tempfile
import tomllib
from pathlib import Path

# A caller token is a bearer secret on the LAN. Short tokens are easy to guess.
MIN_TOKEN_LENGTH = 32

_CALLER_NAME = re.compile(r"[a-z][a-z0-9-]{0,31}")


class SecretError(RuntimeError):
    """A secret file is missing, unsafe, or malformed."""


def _check_mode(path: Path) -> None:
    try:
        info = path.stat()
    except OSError as exc:
        raise SecretError(f"cannot read secret file {path}: {exc}") from exc
    mode = stat.S_IMODE(info.st_mode)
    # A k8s Secret volume file is root:<fsGroup> 0440 or 0640, so a group read by the
    # service's own group is safe. Any other group bit, or any bit for others, is not.
    own_group_read = info.st_gid == os.getegid() and not mode & 0o030
    if mode & 0o007 or (mode & 0o070 and not own_group_read):
        raise SecretError(
            f"secret file {path} is open to another group or to others. Run: chmod 600 {path}"
        )


def read_secret(path: Path) -> str:
    """Return the stripped content of a secret file that only this user or group can read."""
    _check_mode(path)
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise SecretError(f"secret file {path} is empty. Write the secret into it.")
    return text


def _caller_entry(name: object, token: object, path: Path) -> tuple[str, str]:
    if not isinstance(name, str) or not _CALLER_NAME.fullmatch(name):
        raise SecretError(f"{path}: caller name {name!r} must be lowercase letters, digits, '-'")
    if not isinstance(token, str):
        raise SecretError(f"{path}: token for {name} must be a string")
    if len(token) < MIN_TOKEN_LENGTH:
        raise SecretError(
            f"{path}: token for {name} must have at least {MIN_TOKEN_LENGTH} characters. "
            "Make one with: uv run python3 -c 'import secrets; print(secrets.token_urlsafe(32))'"
        )
    return token, name


def load_callers(path: Path) -> dict[str, str]:
    """Return a map of bearer token to caller name from the [callers] table of ``path``."""
    _check_mode(path)
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise SecretError(f"{path} is not valid TOML: {exc}") from exc
    table = data.get("callers")
    if not isinstance(table, dict):
        raise SecretError(f"{path}: missing [callers] table")
    if not table:
        raise SecretError(f'{path}: no callers. Add one line per caller: name = "token"')
    callers: dict[str, str] = {}
    for name, token in table.items():
        key, value = _caller_entry(name, token, path)
        if key in callers:
            raise SecretError(f"{path}: {callers[key]} and {value} have the same token")
        callers[key] = value
    return callers


def write_private(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` with mode 0600, through a rename so readers never see half."""
    # mkstemp makes a new 0600 file with a unique name, so two writers never share a temp file.
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)
        raise
