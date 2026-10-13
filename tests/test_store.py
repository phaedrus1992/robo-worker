import json
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from robo_worker.states import IllegalTransition, JobState
from robo_worker.store import Job, JobStore, StoreError, UnknownJob


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        self.now += 1
        return self.now


@pytest.fixture
def store(tmp_path: Path) -> JobStore:
    return JobStore(tmp_path / "state", clock=Clock())


def _job(store: JobStore, repo: str = "o/r") -> Job:
    return store.create(repo=repo, prompt="do it", tags=["lang-python"], caller="agent")


def test_create_then_get_round_trips(store: JobStore) -> None:
    job = _job(store)
    loaded = store.get(job.id)
    assert loaded == job
    assert loaded.state is JobState.QUEUED
    assert loaded.session_id != job.id


def test_transition_saves_state_and_reason(store: JobStore) -> None:
    job = _job(store)
    store.transition(job, JobState.RUNNING)
    store.transition(job, JobState.FAILED, "boom")
    loaded = store.get(job.id)
    assert (loaded.state, loaded.reason) == (JobState.FAILED, "boom")


def test_illegal_transition_does_not_save(store: JobStore) -> None:
    job = _job(store)
    with pytest.raises(IllegalTransition):
        store.transition(job, JobState.DONE, "skip")
    assert store.get(job.id).state is JobState.QUEUED


@pytest.mark.parametrize("bad_id", ["../../etc", "x", "A" * 32, "0" * 31, "0" * 32 + "/"])
def test_bad_job_ids_are_unknown(store: JobStore, bad_id: str) -> None:
    with pytest.raises(UnknownJob):
        store.get(bad_id)


def test_missing_job_is_unknown(store: JobStore) -> None:
    with pytest.raises(UnknownJob):
        store.get("0" * 32)


def test_unknown_job_message_names_the_id() -> None:
    assert str(UnknownJob("abc")) == "no job has id 'abc'"


def test_corrupt_job_file_names_the_file(store: JobStore) -> None:
    job = _job(store)
    (store.job_dir(job.id) / "job.json").write_text("{not json")
    with pytest.raises(StoreError, match=r"job\.json"):
        store.get(job.id)


def test_all_is_ordered_and_filtered(store: JobStore) -> None:
    first, second = _job(store), _job(store)
    store.transition(second, JobState.CANCELLED, "no")
    assert [j.id for j in store.all()] == [first.id, second.id]
    assert [j.id for j in store.all(JobState.QUEUED)] == [first.id]


def test_events_use_a_cursor(store: JobStore) -> None:
    job = _job(store)
    store.append_event(job.id, "started", {"resume": False})
    store.append_event(job.id, "progress", {"phase": "branch"})
    events, cursor = store.events(job.id, 0)
    assert [e["kind"] for e in events] == ["started", "progress"]
    assert cursor == 2
    more, cursor = store.events(job.id, cursor)
    assert more == []
    assert cursor == 2


def test_events_of_unknown_job_raise(store: JobStore) -> None:
    with pytest.raises(UnknownJob):
        store.events("0" * 32, 0)


def test_events_refuse_negative_cursor(store: JobStore) -> None:
    job = _job(store)
    with pytest.raises(ValueError, match="since"):
        store.events(job.id, -1)


def test_usage_lines_append(store: JobStore) -> None:
    store.append_usage({"job_id": "x", "cost_usd": 0.1})
    store.append_usage({"job_id": "y", "cost_usd": 0.2})
    lines = store.usage_path.read_text().splitlines()
    assert [json.loads(line)["job_id"] for line in lines] == ["x", "y"]


def test_a_new_store_reads_existing_jobs(tmp_path: Path) -> None:
    first = JobStore(tmp_path / "state")
    job = first.create(repo="o/r", prompt="p", tags=[], caller="mac")
    assert JobStore(tmp_path / "state").get(job.id) == job


def test_all_skips_and_reports_a_corrupt_job(store: JobStore) -> None:
    good, bad = _job(store), _job(store)
    (store.job_dir(bad.id) / "job.json").write_text("{not json")
    assert [j.id for j in store.all()] == [good.id]
    assert store.unreadable == {bad.id}
    (store.job_dir(bad.id) / "job.json").unlink()
    store.all()
    assert store.unreadable == {bad.id}
    store.job_dir(bad.id).rmdir()
    store.all()
    assert store.unreadable == frozenset()


def test_events_skip_a_partial_line(store: JobStore) -> None:
    job = _job(store)
    store.append_event(job.id, "started", {})
    with (store.job_dir(job.id) / "events.jsonl").open("a") as sink:
        sink.write('{"kind": "torn')
    events, cursor = store.events(job.id, 0)
    assert [e["kind"] for e in events] == ["started"]
    assert cursor == 2


def test_job_file_is_private(store: JobStore) -> None:
    job = _job(store)
    mode = (store.job_dir(job.id) / "job.json").stat().st_mode & 0o777
    assert mode == 0o600


_FINITE = st.floats(allow_nan=False, allow_infinity=False)
_FLAT = st.dictionaries(st.text(max_size=8), st.text(max_size=8) | st.integers() | st.none())
_JOBS = st.builds(
    Job,
    id=st.from_regex(r"[0-9a-f]{32}", fullmatch=True),
    repo=st.text(),
    prompt=st.text(),
    tags=st.lists(st.text()),
    caller=st.text(),
    session_id=st.text(),
    state=st.sampled_from(list(JobState)),
    created_at=_FINITE,
    updated_at=_FINITE,
    reason=st.none() | st.text(),
    started=st.booleans(),
    interruptions=st.integers(min_value=0),
    attempts=st.integers(min_value=0),
    resume_prompt=st.none() | st.text(),
    resets_at=st.none() | _FINITE,
    result=st.none() | _FLAT,
    usage=st.none() | _FLAT,
)


@given(_JOBS)
def test_job_round_trips_through_json(job: Job) -> None:
    assert Job.from_dict(json.loads(json.dumps(job.to_dict()))) == job


@given(_JOBS)
def test_job_without_attempts_loads_with_zero(job: Job) -> None:
    # A job file written before attempts existed has no such key.
    data = job.to_dict()
    del data["attempts"]
    assert Job.from_dict(data).attempts == 0


@given(_JOBS, st.text(min_size=1, max_size=10))
def test_job_refuses_an_unknown_field(job: Job, key: str) -> None:
    data = job.to_dict()
    if key in data:
        return
    data[key] = 1
    with pytest.raises(ValueError, match="unknown fields"):
        Job.from_dict(data)
