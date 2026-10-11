import contextlib
import json

import pytest
from hypothesis import given
from hypothesis import strategies as st

from robo_worker.stream import (
    InitEvent,
    RateLimitEvent,
    ResultEvent,
    StreamError,
    StreamEvent,
    parse_line,
)
from tests.conftest import FIXTURES


def _events(name: str) -> list[StreamEvent]:
    lines = (FIXTURES / name).read_text().splitlines()
    return [e for e in (parse_line(line) for line in lines) if e is not None]


def test_success_fixture() -> None:
    init, limit, result = _events("success.jsonl")
    assert init == InitEvent(session_id="s1", api_key_source="none", output_style="default")
    assert isinstance(limit, RateLimitEvent)
    assert not limit.limited
    assert limit.resets_at == 1791871200
    assert isinstance(result, ResultEvent)
    assert result.structured_output is not None
    assert result.structured_output["status"] == "done"
    assert result.cost_usd == 0.25
    assert "claude-opus-5-5" in result.model_usage


def test_rejected_rate_limit_is_limited() -> None:
    _, limit = _events("rate_limited.jsonl")
    assert isinstance(limit, RateLimitEvent)
    assert limit.limited


def test_error_result_keeps_http_status() -> None:
    _, result = _events("auth_error.jsonl")
    assert isinstance(result, ResultEvent)
    assert (result.is_error, result.api_error_status) == (True, 401)


def test_boolean_http_status_is_not_a_status() -> None:
    event = parse_line('{"type":"result","api_error_status":true}')
    assert isinstance(event, ResultEvent)
    assert event.api_error_status is None


def test_ask_result_text() -> None:
    _, result = _events("ask_success.jsonl")
    assert isinstance(result, ResultEvent)
    assert result.text == "hello"


def test_other_events_are_ignored() -> None:
    assert parse_line('{"type":"assistant","message":{}}') is None


def test_init_without_key_source_is_unknown() -> None:
    event = parse_line('{"type":"system","subtype":"init","session_id":"s"}')
    assert isinstance(event, InitEvent)
    assert event.api_key_source == "unknown"


@pytest.mark.parametrize(
    "line",
    ["not json", "[1, 2]", '{"type":"rate_limit_event","rate_limit_info":"x"}'],
)
def test_malformed_lines_raise(line: str) -> None:
    with pytest.raises(StreamError):
        parse_line(line)


@given(st.dictionaries(st.text(max_size=12), st.none() | st.text(max_size=12) | st.integers()))
def test_any_json_object_parses_or_raises_stream_error(data: dict) -> None:
    with contextlib.suppress(StreamError):
        parse_line(json.dumps(data))
