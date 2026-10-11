"""Job states and the legal moves between them."""

from enum import StrEnum


class JobState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    BLOCKED = "blocked"
    RATE_LIMITED = "rate_limited"
    INTERRUPTED = "interrupted"
    CANCELLED = "cancelled"


TERMINAL = frozenset({JobState.DONE, JobState.FAILED, JobState.CANCELLED})

# A paused job goes back to QUEUED, not to RUNNING, so it waits for its repo queue.
_ALLOWED: dict[JobState, frozenset[JobState]] = {
    JobState.QUEUED: frozenset({JobState.RUNNING, JobState.FAILED, JobState.CANCELLED}),
    JobState.RUNNING: frozenset(
        {
            JobState.DONE,
            JobState.FAILED,
            JobState.BLOCKED,
            JobState.RATE_LIMITED,
            JobState.INTERRUPTED,
            JobState.CANCELLED,
        }
    ),
    JobState.BLOCKED: frozenset({JobState.QUEUED, JobState.CANCELLED}),
    JobState.RATE_LIMITED: frozenset({JobState.QUEUED, JobState.CANCELLED}),
    JobState.INTERRUPTED: frozenset({JobState.QUEUED, JobState.FAILED, JobState.CANCELLED}),
    JobState.DONE: frozenset(),
    JobState.FAILED: frozenset(),
    JobState.CANCELLED: frozenset(),
}

_NO_REASON_NEEDED = frozenset({JobState.QUEUED, JobState.RUNNING})


class IllegalTransition(ValueError):
    """A job cannot move from its current state to the requested state."""


def allowed_targets(state: JobState) -> frozenset[JobState]:
    return _ALLOWED[state]


def check_transition(old: JobState, new: JobState, reason: str | None) -> None:
    """Raise IllegalTransition when ``old`` cannot move to ``new`` with ``reason``."""
    if new not in _ALLOWED[old]:
        raise IllegalTransition(f"a job cannot go from {old} to {new}")
    if new not in _NO_REASON_NEEDED and not reason:
        raise IllegalTransition(f"state {new} needs a reason")
