import pytest
from hypothesis import given
from hypothesis import strategies as st

from robo_worker.states import (
    TERMINAL,
    IllegalTransition,
    JobState,
    allowed_targets,
    check_transition,
)

STATES = st.sampled_from(list(JobState))


def test_terminal_states_have_no_exit() -> None:
    for state in TERMINAL:
        assert allowed_targets(state) == frozenset()


def test_paused_states_go_back_to_queued() -> None:
    for state in (JobState.BLOCKED, JobState.RATE_LIMITED, JobState.INTERRUPTED):
        assert JobState.QUEUED in allowed_targets(state)
        assert JobState.RUNNING not in allowed_targets(state)


@given(STATES, STATES)
def test_transition_is_legal_exactly_when_allowed(old: JobState, new: JobState) -> None:
    reason = "because"
    if new in allowed_targets(old):
        check_transition(old, new, reason)
    else:
        with pytest.raises(IllegalTransition):
            check_transition(old, new, reason)


@given(STATES, STATES)
def test_every_state_but_queued_and_running_needs_a_reason(old: JobState, new: JobState) -> None:
    if new not in allowed_targets(old):
        return
    if new in (JobState.QUEUED, JobState.RUNNING):
        check_transition(old, new, None)
    else:
        with pytest.raises(IllegalTransition, match="needs a reason"):
            check_transition(old, new, None)
