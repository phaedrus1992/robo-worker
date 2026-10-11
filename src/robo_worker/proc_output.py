"""Short, readable text from the output of a failed subprocess."""

# Enough to show the error line of git, llmenv, or claude, and small enough for a job reason.
STDERR_TAIL_CHARS = 500


def stderr_tail(raw: bytes) -> str:
    """Return the last part of ``raw`` as text, for an error message."""
    return raw.decode("utf-8", errors="replace")[-STDERR_TAIL_CHARS:].strip()
