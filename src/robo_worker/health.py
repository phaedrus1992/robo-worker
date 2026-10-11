"""Health report for the service and for the deploy script."""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from robo_worker.config import Config
from robo_worker.scheduler import Pauses
from robo_worker.secret_files import SecretError, read_secret

# `claude setup-token` makes a one-year token:
# https://code.claude.com/docs/en/authentication#generate-a-long-lived-token
TOKEN_LIFETIME_DAYS = 365


@dataclass(frozen=True)
class ServiceState:
    """What the running service knows that the config does not."""

    pauses: Pauses
    running: int
    unreadable_jobs: frozenset[str] = field(default_factory=frozenset)


def _token_checks(cfg: Config, today: date, problems: list[str], warnings: list[str]) -> int:
    try:
        read_secret(cfg.oauth_token_file)
    except SecretError as exc:
        problems.append(str(exc))
    days_left = (cfg.oauth_token_created + timedelta(days=TOKEN_LIFETIME_DAYS) - today).days
    if days_left <= 0:
        problems.append(
            "the claude oauth token is expired. Run `claude setup-token`, replace the token "
            "file, set auth.oauth_token_created, and restart the service."
        )
    elif days_left <= cfg.token_warn_days:
        warnings.append(f"the claude oauth token expires in {days_left} days. Replace it soon.")
    return days_left


def health_report(
    cfg: Config,
    state: ServiceState,
    *,
    today: date,
    which: Callable[[str], str | None],
) -> dict[str, Any]:
    """Return the health of the service. ``ok`` is False when a job cannot run."""
    pauses, unreadable_jobs = state.pauses, state.unreadable_jobs
    problems: list[str] = []
    warnings: list[str] = []
    days_left = _token_checks(cfg, today, problems, warnings)
    for binary in (cfg.claude_bin, cfg.llmenv_bin, cfg.git_bin):
        if which(binary) is None:
            problems.append(f"{binary} is not on PATH")
    if unreadable_jobs:
        problems.append(
            f"job files cannot be read: {sorted(unreadable_jobs)}. Fix or move them under "
            "<state_root>/jobs/."
        )
    for repo, reason in sorted(pauses.repos.items()):
        warnings.append(f"the queue for {repo} is paused: {reason}")
    if pauses.auth:
        problems.append(
            f"all queues are paused for auth: {pauses.auth}. Fix the token, then restart."
        )
    return {
        "ok": not problems,
        "problems": problems,
        "warnings": warnings,
        "running": state.running,
        "token_days_left": days_left,
        "rate_limit_until": pauses.rate_limit_until,
        "paused_repos": dict(pauses.repos),
    }
