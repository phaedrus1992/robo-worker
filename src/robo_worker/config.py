"""Load and check the robo-worker TOML config."""

import string
import tomllib
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

_MAX_PORT = 65535
_SECTIONS = ("server", "paths", "auth", "limits", "llmenv", "k8s", "bins", "git")
_CLONE_FIELDS = frozenset({"owner", "name"})


class ConfigError(ValueError):
    """The config file is missing, malformed, or has a bad value."""


@dataclass(frozen=True)
class Config:
    listen_host: str
    listen_port: int
    public_url: str
    allowed_hosts: tuple[str, ...]
    workspace_root: Path
    state_root: Path
    job_template: Path
    callers_file: Path
    oauth_token_file: Path
    oauth_token_created: date
    token_warn_days: int
    allowed_key_sources: tuple[str, ...]
    max_parallel_jobs: int
    max_parallel_asks: int
    max_job_seconds: int
    max_ask_seconds: int
    max_prepare_seconds: int
    kill_grace_seconds: int
    max_interruptions: int
    rate_limit_retry_seconds: int
    default_tags: tuple[str, ...]
    deny_tags: tuple[str, ...]
    deny_bundles: tuple[str, ...]
    deny_output_styles: tuple[str, ...]
    extra_env: tuple[str, ...]
    claude_bin: str
    llmenv_bin: str
    git_bin: str
    kubectl_bin: str
    k8s_namespace: str
    clone_url_template: str


class _Section:
    """One TOML table, with typed getters that name the key in every error."""

    def __init__(self, data: dict[str, Any], name: str) -> None:
        table = data.get(name)
        if not isinstance(table, dict):
            raise ConfigError(f"config: missing [{name}] table. See config.example.toml.")
        self._table = table
        self._name = name

    def get[T](self, key: str, kind: type[T]) -> T:
        where = f"{self._name}.{key}"
        if key not in self._table:
            raise ConfigError(f"config: missing {where}. See config.example.toml.")
        value = self._table[key]
        # bool is a subclass of int, so `true` must not pass as an int.
        if (isinstance(value, bool) and kind is not bool) or not isinstance(value, kind):
            raise ConfigError(
                f"config: {where} must be {kind.__name__}, got {type(value).__name__}"
            )
        return value

    def day(self, key: str) -> date:
        value = self.get(key, date)
        # datetime is a subclass of date, so a TOML datetime must not pass as a date.
        if isinstance(value, datetime):
            raise ConfigError(
                f"config: {self._name}.{key} must be a date without a time, like 2026-10-12"
            )
        return value

    def text(self, key: str) -> str:
        value = self.get(key, str)
        if not value.strip():
            raise ConfigError(f"config: {self._name}.{key} must not be empty")
        return value

    def positive(self, key: str) -> int:
        value = self.get(key, int)
        if value <= 0:
            raise ConfigError(f"config: {self._name}.{key} must be greater than 0, got {value}")
        return value

    def strings(self, key: str) -> tuple[str, ...]:
        value = self.get(key, list)
        if not all(isinstance(item, str) and item for item in value):
            raise ConfigError(f"config: {self._name}.{key} must be a list of non-empty strings")
        return tuple(value)

    def path(self, key: str) -> Path:
        path = Path(self.get(key, str))
        if not path.is_absolute():
            raise ConfigError(f"config: {self._name}.{key} must be an absolute path, got {path}")
        return path

    def port(self, key: str) -> int:
        value = self.get(key, int)
        if not 1 <= value <= _MAX_PORT:
            raise ConfigError(f"config: {self._name}.{key} must be between 1 and {_MAX_PORT}")
        return value

    def clone_template(self, key: str) -> str:
        return check_clone_template(self.get(key, str), f"{self._name}.{key}")


def check_clone_template(template: str, where: str = "git.clone_url_template") -> str:
    """Return ``template`` when its only fields are plain {owner} and {name}."""
    try:
        fields = [f for f in string.Formatter().parse(template) if f[1] is not None]
    except ValueError as exc:
        raise ConfigError(f"config: {where} is not a valid template: {exc}") from exc
    for _, field, spec, conversion in fields:
        # An attribute, an index, a conversion, or a format spec can fail at format time.
        if field not in _CLONE_FIELDS or spec or conversion:
            raise ConfigError(
                f"config: {where} may use only {{owner}} and {{name}}, got {{{field}}}"
            )
    if {f[1] for f in fields} != _CLONE_FIELDS:
        raise ConfigError(f"config: {where} must contain {{owner}} and {{name}}")
    return template


def _read(path: Path) -> dict[str, Any]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"cannot read config {path}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"config {path} is not valid TOML: {exc}") from exc


def load_config(path: Path) -> Config:
    """Load ``path`` and return a checked Config, or raise ConfigError naming the bad key."""
    data = _read(path)
    server, paths, auth, limits, llmenv, k8s, bins, git = (_Section(data, n) for n in _SECTIONS)
    return Config(
        listen_host=server.get("listen_host", str),
        listen_port=server.port("listen_port"),
        public_url=server.get("public_url", str).rstrip("/"),
        allowed_hosts=server.strings("allowed_hosts"),
        workspace_root=paths.path("workspace_root"),
        state_root=paths.path("state_root"),
        job_template=paths.path("job_template"),
        callers_file=paths.path("callers_file"),
        oauth_token_file=paths.path("oauth_token_file"),
        oauth_token_created=auth.day("oauth_token_created"),
        token_warn_days=auth.positive("token_warn_days"),
        allowed_key_sources=auth.strings("allowed_key_sources"),
        max_parallel_jobs=limits.positive("max_parallel_jobs"),
        max_parallel_asks=limits.positive("max_parallel_asks"),
        max_job_seconds=limits.positive("max_job_seconds"),
        max_ask_seconds=limits.positive("max_ask_seconds"),
        max_prepare_seconds=limits.positive("max_prepare_seconds"),
        kill_grace_seconds=limits.positive("kill_grace_seconds"),
        max_interruptions=limits.positive("max_interruptions"),
        rate_limit_retry_seconds=limits.positive("rate_limit_retry_seconds"),
        default_tags=llmenv.strings("default_tags"),
        deny_tags=llmenv.strings("deny_tags"),
        deny_bundles=llmenv.strings("deny_bundles"),
        deny_output_styles=llmenv.strings("deny_output_styles"),
        extra_env=llmenv.strings("extra_env"),
        claude_bin=bins.get("claude", str),
        llmenv_bin=bins.get("llmenv", str),
        git_bin=bins.get("git", str),
        kubectl_bin=bins.get("kubectl", str),
        k8s_namespace=k8s.text("namespace"),
        clone_url_template=git.clone_template("clone_url_template"),
    )
