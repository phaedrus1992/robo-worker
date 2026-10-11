"""Parse claude -p stream-json lines into the few events the service needs."""

import json
from dataclasses import dataclass
from typing import Any

# A real run showed "allowed_warning". The refused value is an assumption until the host
# smoke test records one; the runner also treats a result with HTTP 429 as the limit.
LIMITED_STATUSES = frozenset({"rejected"})


class StreamError(ValueError):
    """A stream line is not the JSON the service expects."""


@dataclass(frozen=True)
class InitEvent:
    session_id: str
    api_key_source: str
    output_style: str


@dataclass(frozen=True)
class RateLimitEvent:
    status: str
    resets_at: float | None
    limit_type: str | None

    @property
    def limited(self) -> bool:
        return self.status in LIMITED_STATUSES


@dataclass(frozen=True)
class ResultEvent:
    subtype: str
    is_error: bool
    api_error_status: int | None
    session_id: str
    text: str | None
    structured_output: dict[str, Any] | None
    cost_usd: float | None
    usage: dict[str, Any]
    model_usage: dict[str, Any]


type StreamEvent = InitEvent | RateLimitEvent | ResultEvent


def _str(data: dict[str, Any], key: str, default: str = "") -> str:
    value = data.get(key)
    return value if isinstance(value, str) else default


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def _int(value: object) -> int | None:
    # bool is a subclass of int, so `true` must not pass as a status code.
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _init(data: dict[str, Any]) -> InitEvent:
    return InitEvent(
        session_id=_str(data, "session_id"),
        api_key_source=_str(data, "apiKeySource", "unknown"),
        output_style=_str(data, "output_style", "unknown"),
    )


def _rate_limit(data: dict[str, Any]) -> RateLimitEvent:
    info = data.get("rate_limit_info")
    if not isinstance(info, dict):
        raise StreamError("rate_limit_event has no rate_limit_info object")
    limit_type = info.get("rateLimitType")
    return RateLimitEvent(
        status=_str(info, "status", "unknown"),
        resets_at=_number(info.get("resetsAt")),
        limit_type=limit_type if isinstance(limit_type, str) else None,
    )


def _result(data: dict[str, Any]) -> ResultEvent:
    output = data.get("structured_output")
    text = data.get("result")
    return ResultEvent(
        subtype=_str(data, "subtype", "unknown"),
        is_error=data.get("is_error") is True,
        api_error_status=_int(data.get("api_error_status")),
        session_id=_str(data, "session_id"),
        text=text if isinstance(text, str) else None,
        structured_output=output if isinstance(output, dict) else None,
        cost_usd=_number(data.get("total_cost_usd")),
        usage=_dict(data.get("usage")),
        model_usage=_dict(data.get("modelUsage")),
    )


def parse_line(line: str) -> StreamEvent | None:
    """Return the event in ``line``, or None for an event type the service does not use."""
    try:
        data = json.loads(line)
    except json.JSONDecodeError as exc:
        raise StreamError(f"stream line is not JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise StreamError("stream line is not a JSON object")
    kind = data.get("type")
    if kind == "system" and data.get("subtype") == "init":
        return _init(data)
    if kind == "rate_limit_event":
        return _rate_limit(data)
    if kind == "result":
        return _result(data)
    return None
