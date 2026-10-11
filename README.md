# robo-worker

robo-worker runs `claude -p` jobs on repo checkouts and reports them over MCP.
It is a personal tool for one person's own Claude subscription.

Design: RoboPhaedrus1992/agent `docs/superpowers/specs/2026-10-11-robo-worker-design.md`.

## Development

    uv sync
    uv run pytest
    uv run ruff check && uv run ruff format --check && uv run ty check
