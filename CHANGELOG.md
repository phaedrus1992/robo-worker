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
- Add the `robo-worker` CLI, a systemd unit, a deploy script with rollback, and host setup docs [feat]

[Unreleased]: https://github.com/phaedrus1992/robo-worker/commits/main
