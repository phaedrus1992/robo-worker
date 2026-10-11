import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from robo_worker.asker import Asker, AskError
from robo_worker.config import Config
from robo_worker.scheduler import Pauses
from robo_worker.store import JobStore
from tests.conftest import FIXTURES

pytestmark = pytest.mark.anyio


def _asker(
    config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fixture: str, **extra: str
) -> tuple[Asker, Pauses]:
    monkeypatch.setenv("FAKE_CLAUDE_FIXTURES", str(FIXTURES / fixture))
    monkeypatch.setenv("FAKE_CLAUDE_RECORD", str(tmp_path / "record.jsonl"))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-or-v1-must-not-leak")
    for name, value in extra.items():
        monkeypatch.setenv(name, value)
    pauses = Pauses()
    return Asker(config, JobStore(config.state_root), pauses), pauses


async def test_ask_returns_text_and_usage(
    config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    asker, _ = _asker(config, monkeypatch, tmp_path, "ask_success.jsonl")
    result = await asker.ask("say hello", "be brief", "agent")
    assert result.text == "hello"
    assert result.usage["cost_usd"] == 0.01
    call = json.loads((tmp_path / "record.jsonl").read_text())
    assert call["stdin"] == "say hello"
    argv = call["argv"]
    assert "be brief" not in argv
    assert call["system_file_text"] == "be brief"
    assert list((config.state_root / "ask").glob("system-*")) == []
    assert argv[-2:] == ["--tools", ""]
    assert "--strict-mcp-config" in argv
    assert "--no-session-persistence" in argv
    assert "--session-id" not in argv
    assert not [n for n in call["env"] if n.startswith("ANTHROPIC_")]
    usage = json.loads((config.state_root / "usage.jsonl").read_text())
    assert (usage["kind"], usage["caller"]) == ("ask", "agent")


async def test_ask_without_system_prompt(
    config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    asker, _ = _asker(config, monkeypatch, tmp_path, "ask_success.jsonl")
    assert "--system-prompt-file" not in asker.argv(None)


async def test_empty_prompt(
    config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    asker, _ = _asker(config, monkeypatch, tmp_path, "ask_success.jsonl")
    with pytest.raises(AskError, match="prompt is empty"):
        await asker.ask(" ", None, "agent")


async def test_auth_pause_blocks_asks(
    config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    asker, pauses = _asker(config, monkeypatch, tmp_path, "ask_success.jsonl")
    pauses.auth = "token expired"
    with pytest.raises(AskError, match="token expired"):
        await asker.ask("hi", None, "agent")


async def test_http_401_sets_the_auth_pause(
    config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    asker, pauses = _asker(config, monkeypatch, tmp_path, "auth_error.jsonl")
    with pytest.raises(AskError, match="401"):
        await asker.ask("hi", None, "agent")
    assert pauses.auth is not None


@pytest.mark.parametrize(
    ("fixture", "message"),
    [
        ("rate_limited.jsonl", "rate limit"),
        ("api_key.jsonl", "ANTHROPIC_API_KEY"),
        ("crash.jsonl", "no result"),
    ],
)
async def test_ask_failures(
    config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fixture: str, message: str
) -> None:
    asker, _ = _asker(config, monkeypatch, tmp_path, fixture)
    with pytest.raises(AskError, match=message):
        await asker.ask("hi", None, "agent")


async def test_ask_timeout(config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    short = replace(config, max_ask_seconds=1)
    asker, _ = _asker(short, monkeypatch, tmp_path, "crash.jsonl", FAKE_CLAUDE_SLEEP="30")
    with pytest.raises(AskError, match="max_ask_seconds"):
        await asker.ask("hi", None, "agent")


async def test_missing_token(
    config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    asker, _ = _asker(config, monkeypatch, tmp_path, "ask_success.jsonl")
    os.remove(config.oauth_token_file)
    with pytest.raises(AskError, match="cannot read"):
        await asker.ask("hi", None, "agent")


async def test_malformed_stream_line_is_logged(
    config: Config,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fixture = tmp_path / "noisy.jsonl"
    fixture.write_text("not json\n" + (FIXTURES / "ask_success.jsonl").read_text())
    asker, _ = _asker(config, monkeypatch, tmp_path, str(fixture))
    result = await asker.ask("hi", None, "agent")
    assert result.text == "hello"
    assert "stream_error" in caplog.text


async def test_ask_without_init_fails(
    config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fixture = tmp_path / "no_init.jsonl"
    lines = (FIXTURES / "ask_success.jsonl").read_text().splitlines()
    fixture.write_text("\n".join(line for line in lines if '"subtype":"init"' not in line))
    asker, _ = _asker(config, monkeypatch, tmp_path, str(fixture))
    with pytest.raises(AskError, match="init event"):
        await asker.ask("hi", None, "agent")


async def test_no_result_error_carries_stderr(
    config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    asker, _ = _asker(
        config, monkeypatch, tmp_path, "crash.jsonl", FAKE_CLAUDE_STDERR="Error: bad flag"
    )
    with pytest.raises(AskError, match="bad flag"):
        await asker.ask("hi", None, "agent")


async def test_missing_claude_names_the_config_key(
    config: Config, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    missing = replace(config, claude_bin=str(tmp_path / "no-claude"))
    asker, _ = _asker(missing, monkeypatch, tmp_path, "ask_success.jsonl")
    with pytest.raises(AskError, match=r"bins\.claude"):
        await asker.ask("hi", None, "agent")
