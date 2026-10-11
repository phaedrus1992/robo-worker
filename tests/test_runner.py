import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import anyio
import pytest

from robo_worker import runner
from robo_worker.runner import RunOutcome, RunSettings, RunSpec, run_claude
from tests.conftest import FIXTURES

pytestmark = pytest.mark.anyio
SESSION = "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def settings(fake_claude: Path) -> RunSettings:
    return RunSettings(
        claude_bin=str(fake_claude),
        max_seconds=20,
        kill_grace_seconds=1,
        allowed_key_sources=frozenset({"none"}),
        deny_output_styles=frozenset({"ben"}),
    )


def _spec(tmp_path: Path, *fixtures: str | Path, prompt: str = "do it", **env: str) -> RunSpec:
    mcp = tmp_path / "mcp.json"
    mcp.write_text("{}")
    paths = [str(f if isinstance(f, Path) else FIXTURES / f) for f in fixtures]
    return RunSpec(
        job_id="j1",
        cwd=tmp_path,
        env={
            "PATH": os.environ["PATH"],
            "FAKE_CLAUDE_FIXTURES": os.pathsep.join(paths),
            "FAKE_CLAUDE_RECORD": str(tmp_path / "record.jsonl"),
            **env,
        },
        prompt=prompt,
        session_id=SESSION,
        resume=False,
        mcp_config_path=mcp,
        stream_path=tmp_path / "stream.jsonl",
        stderr_path=tmp_path / "stderr.log",
    )


def _record(tmp_path: Path) -> list[dict[str, Any]]:
    lines = (tmp_path / "record.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


async def _run(
    spec: RunSpec, settings: RunSettings, cancel: anyio.Event | None = None
) -> tuple[RunOutcome, list[tuple[str, dict[str, Any]]]]:
    events: list[tuple[str, dict[str, Any]]] = []
    with anyio.fail_after(15):
        outcome = await run_claude(
            spec,
            settings,
            cancel=cancel or anyio.Event(),
            on_event=lambda kind, data: events.append((kind, data)),
        )
    return outcome, events


async def test_success(tmp_path: Path, settings: RunSettings) -> None:
    outcome, events = await _run(_spec(tmp_path, "success.jsonl"), settings)
    assert outcome.kind == "done"
    assert outcome.reason == "opened PR"
    assert outcome.result is not None
    assert outcome.result["pr_url"] == "https://github.com/o/r/pull/1"
    assert outcome.usage is not None
    assert outcome.usage["cost_usd"] == 0.25
    limit = {"status": "allowed_warning", "resets_at": 1791871200.0, "type": "seven_day"}
    assert ("rate_limit", limit) in events
    assert len((tmp_path / "stream.jsonl").read_text().splitlines()) == 5


async def test_argv_and_stdin(tmp_path: Path, settings: RunSettings) -> None:
    prompt = "--dangerously-skip-permissions and then do the task"
    await _run(_spec(tmp_path, "success.jsonl", prompt=prompt), settings)
    (call,) = _record(tmp_path)
    argv = call["argv"]
    assert call["stdin"] == prompt
    assert prompt not in argv
    assert "--dangerously-skip-permissions" not in argv
    assert "--bare" not in argv
    assert argv[argv.index("--session-id") + 1] == SESSION
    assert argv[argv.index("--output-format") + 1] == "stream-json"
    assert "--verbose" in argv
    assert argv[argv.index("--permission-mode") + 1] == "bypassPermissions"
    assert json.loads(argv[argv.index("--json-schema") + 1]) == runner.RESULT_SCHEMA
    assert argv[argv.index("--setting-sources") + 1] == "user"
    assert argv[-2:] == ["--mcp-config", str(tmp_path / "mcp.json")]


async def test_resume_uses_resume_flag(tmp_path: Path, settings: RunSettings) -> None:
    await _run(replace(_spec(tmp_path, "success.jsonl"), resume=True), settings)
    argv = _record(tmp_path)[0]["argv"]
    assert argv[argv.index("--resume") + 1] == SESSION
    assert "--session-id" not in argv


async def test_blocked(tmp_path: Path, settings: RunSettings) -> None:
    outcome, _ = await _run(_spec(tmp_path, "blocked.jsonl"), settings)
    assert (outcome.kind, outcome.reason) == ("blocked", "Which base branch?")


async def test_rejected_rate_limit_without_result(tmp_path: Path, settings: RunSettings) -> None:
    outcome, _ = await _run(_spec(tmp_path, "rate_limited.jsonl"), settings)
    assert (outcome.kind, outcome.resets_at) == ("rate_limited", 1791871200.0)


async def test_http_429_is_rate_limited(tmp_path: Path, settings: RunSettings) -> None:
    outcome, _ = await _run(_spec(tmp_path, "http_429.jsonl"), settings)
    assert outcome.kind == "rate_limited"


async def test_http_401_is_auth(tmp_path: Path, settings: RunSettings) -> None:
    outcome, _ = await _run(_spec(tmp_path, "auth_error.jsonl"), settings)
    assert outcome.kind == "auth"


async def test_crash_names_exit_code(tmp_path: Path, settings: RunSettings) -> None:
    outcome, _ = await _run(_spec(tmp_path, "crash.jsonl", FAKE_CLAUDE_EXIT="3"), settings)
    assert outcome.kind == "failed"
    assert "code 3" in outcome.reason
    assert "stderr.log" in outcome.reason


async def test_bad_result_fails(tmp_path: Path, settings: RunSettings) -> None:
    outcome, _ = await _run(_spec(tmp_path, "bad_result.jsonl"), settings)
    assert outcome.kind == "failed"
    assert "maybe" in outcome.reason


@pytest.mark.parametrize(
    ("fixture", "word"), [("api_key.jsonl", "ANTHROPIC_API_KEY"), ("ben_style.jsonl", "ben")]
)
async def test_policy_violation_stops_the_process(
    tmp_path: Path, settings: RunSettings, fixture: str, word: str
) -> None:
    spec = _spec(tmp_path, fixture, FAKE_CLAUDE_SLEEP="30")
    with anyio.fail_after(10):
        outcome, _ = await _run(spec, settings)
    assert outcome.kind == "failed"
    assert word in outcome.reason


async def test_timeout(tmp_path: Path, settings: RunSettings) -> None:
    spec = _spec(tmp_path, "crash.jsonl", FAKE_CLAUDE_SLEEP="30")
    outcome, _ = await _run(spec, replace(settings, max_seconds=1))
    assert outcome.kind == "timeout"
    assert "max_job_seconds" in outcome.reason


async def test_timeout_covers_a_prompt_that_is_never_read(
    tmp_path: Path, settings: RunSettings
) -> None:
    big = "x" * (1024 * 1024)
    spec = _spec(
        tmp_path, "crash.jsonl", prompt=big, FAKE_CLAUDE_SKIP_STDIN="1", FAKE_CLAUDE_SLEEP="30"
    )
    outcome, _ = await _run(spec, replace(settings, max_seconds=1))
    assert outcome.kind == "timeout"


async def _cancel_soon(cancel: anyio.Event) -> None:
    await anyio.sleep(0.5)
    cancel.set()


@pytest.mark.parametrize("ignore_term", ["", "1"])
async def test_cancel(tmp_path: Path, settings: RunSettings, ignore_term: str) -> None:
    spec = _spec(
        tmp_path, "crash.jsonl", FAKE_CLAUDE_SLEEP="30", FAKE_CLAUDE_IGNORE_TERM=ignore_term
    )
    cancel = anyio.Event()
    async with anyio.create_task_group() as tg:
        tg.start_soon(_cancel_soon, cancel)
        outcome, _ = await _run(spec, settings, cancel)
    assert outcome.kind == "cancelled"


async def test_last_line_without_newline(tmp_path: Path, settings: RunSettings) -> None:
    fixture = tmp_path / "no_newline.jsonl"
    fixture.write_text((FIXTURES / "success.jsonl").read_text().rstrip("\n"))
    outcome, _ = await _run(_spec(tmp_path, fixture), settings)
    assert outcome.kind == "done"


async def test_overlong_line_stops_the_job(
    tmp_path: Path, settings: RunSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(runner, "MAX_LINE_BYTES", 100)
    fixture = tmp_path / "long.jsonl"
    fixture.write_text('{"type":"assistant","x":"' + "y" * 500)
    outcome, _ = await _run(_spec(tmp_path, fixture, FAKE_CLAUDE_SLEEP="30"), settings)
    assert outcome.kind == "failed"
    assert "longer than" in outcome.reason


async def test_missing_binary(tmp_path: Path, settings: RunSettings) -> None:
    outcome, _ = await _run(
        _spec(tmp_path, "success.jsonl"), replace(settings, claude_bin=str(tmp_path / "none"))
    )
    assert outcome.kind == "failed"
    assert "cannot start" in outcome.reason


async def test_result_without_init_fails(tmp_path: Path, settings: RunSettings) -> None:
    fixture = tmp_path / "no_init.jsonl"
    lines = (FIXTURES / "success.jsonl").read_text().splitlines()
    fixture.write_text("\n".join(line for line in lines if '"subtype":"init"' not in line))
    outcome, _ = await _run(_spec(tmp_path, fixture), settings)
    assert outcome.kind == "failed"
    assert "init event" in outcome.reason


async def test_exit_without_any_event_names_init(tmp_path: Path, settings: RunSettings) -> None:
    fixture = tmp_path / "empty.jsonl"
    fixture.write_text("")
    outcome, _ = await _run(_spec(tmp_path, fixture), settings)
    assert outcome.kind == "failed"
    assert "init event" in outcome.reason


async def test_error_in_event_sink_still_stops_claude(
    tmp_path: Path, settings: RunSettings
) -> None:
    pidfile = tmp_path / "pid"
    spec = _spec(
        tmp_path, "success.jsonl", FAKE_CLAUDE_SLEEP="30", FAKE_CLAUDE_PIDFILE=str(pidfile)
    )

    def broken_sink(kind: str, data: dict[str, Any]) -> None:
        raise OSError("disk full")

    with anyio.fail_after(10), pytest.raises(ExceptionGroup):
        await run_claude(spec, settings, cancel=anyio.Event(), on_event=broken_sink)
    with pytest.raises(ProcessLookupError):
        os.kill(int(pidfile.read_text()), 0)
