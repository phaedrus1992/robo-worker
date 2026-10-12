"""File-backed job records, events, and usage lines."""

import json
import logging
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any

from robo_worker.secret_files import write_private
from robo_worker.states import JobState, check_transition

_log = logging.getLogger(__name__)

# Shape of uuid4().hex. A strict match keeps a caller's job id from becoming a path.
_JOB_ID = re.compile(r"[0-9a-f]{32}")


class UnknownJob(KeyError):
    """No job has this id."""

    def __str__(self) -> str:
        # KeyError shows only the quoted key, and MCP callers see this text as the error.
        return f"no job has id {self.args[0]!r}"


class StoreError(RuntimeError):
    """A job file exists but cannot be read."""


@dataclass
class Job:
    id: str
    repo: str
    prompt: str
    tags: list[str]
    caller: str
    session_id: str
    state: JobState
    created_at: float
    updated_at: float
    reason: str | None = None
    started: bool = False
    interruptions: int = 0
    # Pod runs so far. The launcher names each k8s Job from it, so no two runs share a name.
    attempts: int = 0
    resume_prompt: str | None = None
    resets_at: float | None = None
    result: dict[str, Any] | None = None
    usage: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["state"] = self.state.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Job:
        known = {f.name for f in fields(cls)}
        unknown = sorted(set(data) - known)
        if unknown:
            raise ValueError(f"unknown fields {unknown}")
        values: dict[str, Any] = dict(data)
        values["state"] = JobState(data["state"])
        return cls(**values)


class JobStore:
    """One directory per job under ``root/jobs``, plus ``root/usage.jsonl``."""

    def __init__(self, root: Path, clock: Callable[[], float] = time.time) -> None:
        self._jobs = root / "jobs"
        self._jobs.mkdir(parents=True, exist_ok=True)
        self._usage = root / "usage.jsonl"
        self._clock = clock
        self._unreadable: set[str] = set()

    @property
    def usage_path(self) -> Path:
        return self._usage

    @property
    def unreadable(self) -> frozenset[str]:
        """Ids of the job files that the last call to all() could not read."""
        return frozenset(self._unreadable)

    def job_dir(self, job_id: str) -> Path:
        if not isinstance(job_id, str) or not _JOB_ID.fullmatch(job_id):
            raise UnknownJob(job_id)
        return self._jobs / job_id

    def stream_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "stream.jsonl"

    def stderr_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "stderr.log"

    def create(self, *, repo: str, prompt: str, tags: list[str], caller: str) -> Job:
        now = self._clock()
        job = Job(
            id=uuid.uuid4().hex,
            repo=repo,
            prompt=prompt,
            tags=list(tags),
            caller=caller,
            session_id=str(uuid.uuid4()),
            state=JobState.QUEUED,
            created_at=now,
            updated_at=now,
        )
        self.job_dir(job.id).mkdir()
        self.save(job)
        return job

    def get(self, job_id: str) -> Job:
        path = self.job_dir(job_id) / "job.json"
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            raise UnknownJob(job_id) from None
        try:
            return Job.from_dict(json.loads(text))
        except (ValueError, TypeError, KeyError) as exc:
            raise StoreError(f"cannot read {path}: {exc}. Fix or move the file.") from exc

    def save(self, job: Job) -> None:
        job.updated_at = self._clock()
        # Mode 0600: job.json holds the caller prompt and the result.
        write_private(self.job_dir(job.id) / "job.json", json.dumps(job.to_dict(), indent=2))

    def transition(self, job: Job, new: JobState, reason: str | None = None) -> Job:
        check_transition(job.state, new, reason)
        job.state = new
        job.reason = reason
        self.save(job)
        return job

    def all(self, state: JobState | None = None) -> list[Job]:
        """Return the readable jobs, oldest first. Unreadable job files are logged and skipped."""
        jobs: list[Job] = []
        unreadable: set[str] = set()
        for path in self._jobs.iterdir():
            if not _JOB_ID.fullmatch(path.name):
                continue
            try:
                jobs.append(self.get(path.name))
            except (StoreError, UnknownJob) as exc:
                # One bad file must not stop the queue for every other job.
                unreadable.add(path.name)
                if path.name not in self._unreadable:
                    _log.error("skipping job %s: %s", path.name, exc)
        self._unreadable = unreadable
        jobs.sort(key=lambda j: j.created_at)
        return [j for j in jobs if state is None or j.state is state]

    def append_event(self, job_id: str, kind: str, data: dict[str, Any]) -> None:
        line = json.dumps({"time": self._clock(), "kind": kind, "data": data})
        with (self.job_dir(job_id) / "events.jsonl").open("a", encoding="utf-8") as sink:
            sink.write(line + "\n")

    def events(self, job_id: str, since: int) -> tuple[list[dict[str, Any]], int]:
        """Return the events after cursor ``since`` and the next cursor."""
        if since < 0:
            raise ValueError(f"since must be 0 or more, got {since}")
        path = self.job_dir(job_id) / "events.jsonl"
        if not path.exists():
            self.get(job_id)
            return [], 0
        lines = path.read_text(encoding="utf-8").splitlines()
        return [e for e in map(self._event, lines[since:]) if e is not None], len(lines)

    def _event(self, line: str) -> dict[str, Any] | None:
        # A crash during an append can leave a partial last line. The cursor still counts it.
        try:
            event = json.loads(line)
        except ValueError:
            _log.warning("skipping an unreadable event line: %.80r", line)
            return None
        return event if isinstance(event, dict) else None

    def append_usage(self, record: dict[str, Any]) -> None:
        with self._usage.open("a", encoding="utf-8") as sink:
            sink.write(json.dumps(record) + "\n")
