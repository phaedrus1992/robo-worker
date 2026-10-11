"""Build the worker environment: llmenv export, checks, and an allowlist."""

import json
import re
import shlex
from collections.abc import Mapping, Sequence
from pathlib import Path

import anyio

from robo_worker.proc_output import stderr_tail

# These names change which account claude bills, or which provider it calls. Precedence:
# https://code.claude.com/docs/en/authentication#authentication-precedence
BLOCKED_PREFIXES = ("ANTHROPIC_", "CLAUDE_CODE_USE_")
OAUTH_TOKEN_NAME = "CLAUDE_CODE_OAUTH_TOKEN"
BASE_NAMES = ("HOME", "PATH", "LANG")

_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# Shape only. No comma, because LLMENV_EXTRA_TAGS is a comma-separated list.
_TAG = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,63}")


class EnvError(RuntimeError):
    """The worker environment cannot be built or is not safe to use."""


class TagError(ValueError):
    """A job tag is malformed."""


def validate_tags(tags: object) -> list[str]:
    """Return ``tags`` as a list, or raise TagError naming the bad tag."""
    if not isinstance(tags, list):
        raise TagError(f"tags must be a list of strings, got {type(tags).__name__}")
    for tag in tags:
        if not isinstance(tag, str) or not _TAG.fullmatch(tag):
            raise TagError(f"tag {tag!r} is not valid. Use letters, digits, '.', '_', ':', '-'.")
    return list(tags)


def parse_export(text: str) -> dict[str, str]:
    """Parse the shell ``export NAME='value'`` lines that ``llmenv export`` prints."""
    try:
        tokens = shlex.split(text, posix=True)
    except ValueError as exc:
        raise EnvError(f"cannot parse llmenv export output: {exc}") from exc
    result: dict[str, str] = {}
    for token in tokens:
        if token == "export":
            continue
        name, sep, value = token.partition("=")
        if not sep or not _ENV_NAME.fullmatch(name):
            raise EnvError(f"llmenv export printed an unexpected token {token[:60]!r}")
        result[name] = value
    return result


def _blocked(name: str) -> bool:
    return name.startswith(BLOCKED_PREFIXES) or name == OAUTH_TOKEN_NAME


def build_worker_env(
    *,
    parent: Mapping[str, str],
    llmenv_vars: Mapping[str, str],
    oauth_token: str,
    extra_env: Sequence[str],
) -> dict[str, str]:
    """Return the worker environment. Nothing from ``parent`` passes unless allowlisted."""
    env = {
        name: parent[name]
        for name in (*BASE_NAMES, *extra_env)
        if name in parent and not _blocked(name)
    }
    env.update({name: value for name, value in llmenv_vars.items() if not _blocked(name)})
    env[OAUTH_TOKEN_NAME] = oauth_token
    return env


def check_bundles(llmenv_vars: Mapping[str, str], deny_bundles: Sequence[str]) -> None:
    """Raise EnvError when llmenv activated a bundle in ``deny_bundles``."""
    raw = llmenv_vars.get("LLMENV_ACTIVE_BUNDLES", "")
    active = {name.strip() for name in raw.split(",") if name.strip()}
    denied = sorted(active & set(deny_bundles))
    if denied:
        raise EnvError(
            f"llmenv activated denied bundles {denied}. "
            "Change the job tags, or the deny_bundles config key."
        )


_SETTINGS_FILES = ("settings.json", "settings.local.json")


def check_claude_settings(llmenv_vars: Mapping[str, str], home: str | None) -> None:
    """Raise EnvError when the user settings could make claude bill another account.

    apiKeyHelper outranks the plan token, and a settings ``env`` map can set the same
    names that the worker allowlist removes.
    """
    config_dir = llmenv_vars.get("CLAUDE_CONFIG_DIR")
    if not config_dir and home:
        config_dir = str(Path(home) / ".claude")
    if not config_dir:
        return
    for name in _SETTINGS_FILES:
        _check_settings_file(Path(config_dir) / name)


def _check_settings_file(settings: Path) -> None:
    try:
        data = json.loads(settings.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return
    except (OSError, ValueError) as exc:
        raise EnvError(f"cannot read {settings}: {exc}") from exc
    if not isinstance(data, dict):
        return
    if "apiKeyHelper" in data:
        raise EnvError(
            f"{settings} sets apiKeyHelper, which takes precedence over the plan token. "
            "Remove it from the llmenv bundles of this worker."
        )
    env = data.get("env")
    blocked = sorted(n for n in env if _blocked(n)) if isinstance(env, dict) else []
    if blocked:
        raise EnvError(
            f"{settings} sets {blocked} in its env map, which changes the account claude "
            "bills. Remove them from the llmenv bundles of this worker."
        )


async def llmenv_export(
    llmenv_bin: str, cwd: Path, tags: Sequence[str], parent: Mapping[str, str], timeout: float
) -> dict[str, str]:
    """Run ``llmenv export`` in ``cwd`` with ``tags`` added, and return its variables."""
    env = {**parent, "LLMENV_EXTRA_TAGS": ", ".join(tags)}
    where = f"in {cwd} with tags {list(tags)}"
    try:
        with anyio.fail_after(timeout):
            result = await anyio.run_process([llmenv_bin, "export"], cwd=cwd, env=env, check=False)
    except TimeoutError:
        raise EnvError(
            f"llmenv export {where} ran longer than max_prepare_seconds ({timeout}s)"
        ) from None
    except OSError as exc:
        raise EnvError(
            f"cannot run {llmenv_bin}: {exc}. Check the bins.llmenv config key."
        ) from exc
    if result.returncode != 0:
        raise EnvError(
            f"llmenv export failed {where}: exit {result.returncode}: {stderr_tail(result.stderr)}"
        )
    return parse_export(result.stdout.decode("utf-8", errors="replace"))
