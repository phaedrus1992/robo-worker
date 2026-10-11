# robo-worker

robo-worker runs `claude -p` jobs on repo checkouts and reports them over MCP.
It is a personal tool for one person's own Claude subscription.

Design: RoboPhaedrus1992/agent `docs/superpowers/specs/2026-10-11-robo-worker-design.md`.

## Development

    uv sync
    uv run pytest
    uv run ruff check && uv run ruff format --check && uv run ty check

## Trust model

- All callers are one trust domain: one person's agent and that person's own sessions.
  Any caller can read, cancel, or resume any job, and `list_jobs` shows every prompt.
- A worker runs `claude -p` with `bypassPermissions` and runs the code of the repo it checks out, such as its tests.
  That code sees the plan OAuth token in its environment.
  Submit only repos that you trust.
- The server and its workers share the unix user `robo-worker`, so a worker can read the caller tokens and change the service files.
  Issue #1 tracks a separate user for the workers.
- `default_tags`, `deny_tags`, and `deny_bundles` are denylists.
  A tag or bundle that the llmenv config adds later is open to jobs until you deny it.

## Operations

- Host setup: `docs/host-setup.md`.
- Deploy: `scripts/deploy.sh <ssh-host> http://<lan-address>:<port>/health [git-ref]`.
  The script refuses to restart while jobs run, or when it cannot read the job count, unless you pass `--force`.
  Exit codes: 0 deployed, 2 usage, 3 rolled back, 4 rollback failed, 5 refused, 6 nothing changed (cannot read or fetch on the host).
- Health: `GET /health` needs no token. It returns 503 when a job cannot run.
- Job files: `<state_root>/jobs/<id>/` holds `job.json`, `events.jsonl`, `stream.jsonl`, `stderr.log`, and `mcp.json`.
- Usage: `<state_root>/usage.jsonl` has one line for each job run and each ask.
- A paused repo queue (`checkout_dirty`): clean the checkout under `workspace_root`, then call `resume_queue`.
- An auth pause: replace the token file, update `auth.oauth_token_created`, and restart the unit.
