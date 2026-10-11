# robo-worker

robo-worker runs `claude -p` jobs on repo checkouts and reports them over MCP.
It is a personal tool for one person's own Claude subscription.

Design: RoboPhaedrus1992/agent `docs/superpowers/specs/2026-10-11-robo-worker-design.md`.

## Development

    uv sync
    uv run pytest
    uv run ruff check && uv run ruff format --check && uv run ty check

## Operations

- Host setup: `docs/host-setup.md`.
- Deploy: `scripts/deploy.sh <ssh-host> http://<lan-address>:<port>/health [git-ref]`.
  The script refuses to restart while jobs run, unless you pass `--force`.
- Health: `GET /health` needs no token. It returns 503 when a job cannot run.
- Job files: `<state_root>/jobs/<id>/` holds `job.json`, `events.jsonl`, `stream.jsonl`, `stderr.log`, and `mcp.json`.
- Usage: `<state_root>/usage.jsonl` has one line for each job run and each ask.
- A paused repo queue (`checkout_dirty`): clean the checkout under `workspace_root`, then call `resume_queue`.
- An auth pause: replace the token file, update `auth.oauth_token_created`, and restart the unit.
