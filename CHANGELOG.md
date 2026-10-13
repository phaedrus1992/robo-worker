# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Add a TOML config with a documented example, `config.example.toml` [feat]
- Keep job state, events, and usage records as plain files under `state_root`, so a restart recovers every job [feat]
- Build each worker's environment from llmenv tags through an allowlist that drops `ANTHROPIC_*`, `CLAUDE_CODE_USE_*`, and an inherited OAuth token [feat]
- Keep one checkout per repo, and refuse to start a job on a dirty tree [feat]
- Run `claude -p` with the prompt on stdin, and stop a session that uses a disallowed auth source or output style [feat]
- Queue jobs per repo, pause on an auth failure, the plan rate limit, or a dirty checkout, and resume interrupted jobs after a restart [feat]
- Add a text-only `ask` call [feat]
- Serve the job tools over MCP with a bearer token per caller, plus an open `/health` route [feat]
- Run each attempt of a job in its own k8s Job pod, and reattach to a running pod after a control-plane restart instead of stopping it [feat:scheduler]
- Add `robo-worker serve` for the control plane and `robo-worker job` for the job pod [feat:cli]
- Build the `ghcr.io/phaedrus1992/robo-worker` image, with every binary pinned by version and checksum [build]
- Add k8s manifests under `deploy/k8s`: a control-plane Deployment, a NodePort Service, two claims, and a namespaced Role [feat:deploy]
- Deploy with `scripts/deploy.sh [git-ref]`, which refuses while a job pod runs and rolls back to the revision that ran before [feat:deploy]
- Keep each job pod to its own job directory and repo checkout, and never run a program on data that a job pod wrote [fix:security]

[Unreleased]: https://github.com/phaedrus1992/robo-worker/commits/main
