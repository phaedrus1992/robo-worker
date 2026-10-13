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
- Each attempt of a job runs in its own job pod. A job pod mounts only its own job directory and its own repo checkout. It does not mount the caller tokens, and it has no access to the k8s API. The control plane never runs a program on data that a job pod wrote. Details: `docs/deploy.md`, "What a job pod can reach".
- Jobs on the same repo share its checkout, so a job can leave a git hook there for the next job on that repo.
- `default_tags`, `deny_tags`, and `deny_bundles` are denylists.
  A tag or bundle that the llmenv config adds later is open to jobs until you deny it.

## Running it

    robo-worker serve --config <config>                 # the control plane, in the cluster
    robo-worker job --config <config> --job-id <id>     # one task, in a job pod

Deployment, secrets, and operations: `docs/deploy.md`.
