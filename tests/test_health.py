from datetime import date, timedelta

from robo_worker.config import Config
from robo_worker.health import TOKEN_LIFETIME_DAYS, ServiceState, health_report
from robo_worker.scheduler import Pauses


def _which(name: str) -> str | None:
    return f"/usr/bin/{name}"


def test_healthy(config: Config) -> None:
    report = health_report(config, ServiceState(Pauses(), 1), today=date.today(), which=_which)
    assert report["ok"] is True
    assert report["running"] == 1
    assert report["warnings"] == []


def test_token_near_expiry_warns(config: Config) -> None:
    today = config.oauth_token_created + timedelta(days=TOKEN_LIFETIME_DAYS - 5)
    report = health_report(config, ServiceState(Pauses(), 0), today=today, which=_which)
    assert report["ok"] is True
    assert "expires in 5 days" in report["warnings"][0]


def test_expired_token_fails(config: Config) -> None:
    today = config.oauth_token_created + timedelta(days=TOKEN_LIFETIME_DAYS)
    report = health_report(config, ServiceState(Pauses(), 0), today=today, which=_which)
    assert report["ok"] is False
    assert "expired" in report["problems"][0]


def test_missing_binary_and_auth_pause_fail(config: Config) -> None:
    pauses = Pauses(auth="HTTP 401", repos={"o/r": "checkout_dirty: x"})
    report = health_report(
        config, ServiceState(pauses, 0), today=date.today(), which=lambda _: None
    )
    assert report["ok"] is False
    assert any("not on PATH" in p for p in report["problems"])
    assert any("HTTP 401" in p for p in report["problems"])
    assert report["paused_repos"] == {"o/r": "checkout_dirty: x"}


def test_missing_token_file_fails(config: Config) -> None:
    config.oauth_token_file.unlink()
    report = health_report(config, ServiceState(Pauses(), 0), today=date.today(), which=_which)
    assert report["ok"] is False


def test_unreadable_jobs_fail_and_paused_repos_warn(config: Config) -> None:
    pauses = Pauses(repos={"o/r": "checkout_dirty: x"})
    report = health_report(
        config, ServiceState(pauses, 0, frozenset({"ab"})), today=date.today(), which=_which
    )
    assert report["ok"] is False
    assert any("'ab'" in p for p in report["problems"])
    assert report["warnings"] == ["the queue for o/r is paused: checkout_dirty: x"]
