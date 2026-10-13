import contextlib
import json
from pathlib import Path
from typing import get_args

import pytest
from hypothesis import given
from hypothesis import strategies as st

from robo_worker.attempt import (
    MAX_POD_FILE_BYTES,
    AttemptSpec,
    PodFileError,
    PodPaths,
    StagingError,
    outcome_from_json,
    outcome_to_json,
    read_pod_events,
    read_pod_text,
    spec_from_json,
    spec_to_json,
    stage_pod_dir,
)
from robo_worker.runner import OutcomeKind, RunOutcome

_TEXT = st.text(alphabet=st.characters(blacklist_categories=("Cs",)))
_FLAT = st.dictionaries(st.text(max_size=5), st.integers() | st.text(max_size=5), max_size=3)
_OUTCOMES = st.builds(
    RunOutcome,
    kind=st.sampled_from(get_args(OutcomeKind.__value__)),
    reason=_TEXT,
    result=st.none() | _FLAT,
    usage=st.none() | _FLAT,
    resets_at=st.none() | st.floats(allow_nan=False, allow_infinity=False),
    checkout_problem=st.none() | _TEXT,
)
_SPECS = st.builds(
    AttemptSpec,
    job_id=st.from_regex(r"[0-9a-f]{32}", fullmatch=True),
    attempt=st.integers(min_value=1, max_value=10_000),
    repo=st.just("o/r"),
    tags=st.lists(st.from_regex(r"[a-z][a-z0-9-]{0,10}", fullmatch=True), max_size=3),
    prompt=_TEXT,
    session_id=st.uuids().map(str),
    resume=st.booleans(),
)
_JSON = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False) | st.text(max_size=5),
    lambda inner: st.lists(inner, max_size=3) | st.dictionaries(st.text(max_size=5), inner),
    max_leaves=10,
)


@given(_OUTCOMES)
def test_outcome_round_trips_through_json(outcome: RunOutcome) -> None:
    assert outcome_from_json(outcome_to_json(outcome)) == outcome


@given(_SPECS)
def test_spec_round_trips_through_json(spec: AttemptSpec) -> None:
    assert spec_from_json(spec_to_json(spec)) == spec


_KEYS = ["kind", "reason", "result", "usage", "resets_at", "checkout_problem"]


@given(st.fixed_dictionaries({k: _JSON for k in _KEYS}) | _JSON)
def test_outcome_from_json_raises_only_its_own_error(data: object) -> None:
    # A pod writes this file, so any content must give an outcome or a PodFileError.
    with contextlib.suppress(PodFileError):
        outcome_from_json(json.dumps(data))


@given(_JSON)
def test_spec_from_json_raises_only_its_own_error(data: object) -> None:
    with contextlib.suppress(PodFileError):
        spec_from_json(json.dumps(data))


@pytest.mark.parametrize(
    "change",
    [
        {"kind": "won"},
        {"kind": [1]},
        {"reason": 3},
        {"result": [1]},
        {"resets_at": True},
        {"resets_at": "soon"},
        {"checkout_problem": 1},
        {"extra": 1},
    ],
)
def test_outcome_from_json_refuses_a_bad_field(change: dict[str, object]) -> None:
    good = json.loads(outcome_to_json(RunOutcome("done", "ok")))
    with pytest.raises(PodFileError):
        outcome_from_json(json.dumps(good | change))


def test_read_pod_text_does_not_follow_a_link(tmp_path: Path) -> None:
    secret = tmp_path / "callers.toml"
    secret.write_text("token")
    link = tmp_path / "outcome-1.json"
    link.symlink_to(secret)
    with pytest.raises(PodFileError):
        read_pod_text(link)


def test_read_pod_text_refuses_a_large_file(tmp_path: Path) -> None:
    big = tmp_path / "stream.jsonl"
    big.write_bytes(b"x" * (MAX_POD_FILE_BYTES + 1))
    with pytest.raises(PodFileError, match="limit"):
        read_pod_text(big)


def test_read_pod_events_keeps_only_known_kinds(tmp_path: Path) -> None:
    path = tmp_path / "pod-events.jsonl"
    lines = [
        {"kind": "rate_limit", "data": {"status": "allowed_warning"}},
        {"kind": "finished", "data": {"state": "done"}},
        {"kind": "stream_error", "data": "not a dict"},
    ]
    path.write_text("\n".join(json.dumps(x) for x in lines) + "\nnot json\n")
    assert read_pod_events(path) == [("rate_limit", {"status": "allowed_warning"})]


def _master(tmp_path: Path) -> Path:
    master = tmp_path / "master"
    master.mkdir()
    (master / "config.yaml").write_text("scopes: {}\n")
    return master


def test_stage_pod_dir_replaces_links_a_pod_planted(tmp_path: Path) -> None:
    master = _master(tmp_path)
    control_home = tmp_path / "control-home"
    control_home.mkdir()
    paths = PodPaths(tmp_path / "jobs" / "j" / "pod")
    paths.root.mkdir(parents=True)
    # A pod points its HOME at the control plane's HOME, hoping the next copy lands there.
    paths.home.symlink_to(control_home, target_is_directory=True)

    stage_pod_dir(paths, master)

    assert not paths.home.is_symlink()
    assert (paths.llmenv_config / "config.yaml").read_text() == "scopes: {}\n"
    assert list(control_home.iterdir()) == []


def test_stage_pod_dir_gives_each_attempt_a_fresh_copy(tmp_path: Path) -> None:
    master = _master(tmp_path)
    paths = PodPaths(tmp_path / "jobs" / "j" / "pod")
    paths.root.mkdir(parents=True)
    stage_pod_dir(paths, master)
    (paths.llmenv_config / "config.yaml").write_text("changed by a job\n")
    (paths.llmenv_config / "extra").write_text("x")

    stage_pod_dir(paths, master)

    assert (paths.llmenv_config / "config.yaml").read_text() == "scopes: {}\n"
    assert not (paths.llmenv_config / "extra").exists()


def test_stage_pod_dir_needs_the_llmenv_config(tmp_path: Path) -> None:
    paths = PodPaths(tmp_path / "pod")
    with pytest.raises(StagingError, match=r"deploy\.md"):
        stage_pod_dir(paths, tmp_path / "missing")
