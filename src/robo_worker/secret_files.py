"""Read and write secret files with owner-only permissions."""

import os
import re
import stat
import tomllib
from pathlib import Path

# A caller token is a bearer secret on the LAN. Short tokens are easy to guess.
MIN_TOKEN_LENGTH = 32

_CALLER_NAME = re.compile(r"[a-z][a-z0-9-]{0,31}")


class SecretError(RuntimeError):
    """A secret file is missing, unsafe, or malformed."""


def _check_mode(path: Path) -> None:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except OSError as exc:
        raise SecretError(f"cannot read secret file {path}: {exc}") from exc
    if mode & 0o077:
        raise SecretError(
            f"secret file {path} is readable by group or others. Run: chmod 600 {path}"
        )


def read_secret(path: Path) -> str:
    """Return the stripped content of an owner-only secret file."""
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
    tmp = path.with_name(f".{path.name}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    # O_CREAT does not change the mode of a temp file that a crash left behind.
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)
    os.replace(tmp, path)
