#!/usr/bin/env bash
# Usage: scripts/deploy.sh [--force] <ssh-host> <health-url> [git-ref]
#
# Deploy robo-worker on <ssh-host>: check out <git-ref> (default origin/main), run uv sync,
# restart the unit, and check <health-url>. Revert to the previous commit when that fails.
# --force deploys even when jobs run; the restart interrupts them, and they resume later.
#
# Exit codes: 0 deployed, 2 usage, 3 rolled back, 4 rollback failed, 5 refused (jobs running).
set -euo pipefail

APP_DIR="${ROBO_WORKER_APP_DIR:-/home/robo-worker/robo-worker}"
UNIT="${ROBO_WORKER_UNIT:-robo-worker}"
HEALTH_TRIES="${ROBO_WORKER_HEALTH_TRIES:-15}"
HEALTH_DELAY="${ROBO_WORKER_HEALTH_DELAY:-2}"

usage() {
  sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

# The commands go to the remote shell on stdin, so no argument needs a second level of quoting.
as_worker() {
  ssh "$HOST" sudo -u robo-worker -H bash -l -s <<<"set -euo pipefail; cd '$APP_DIR'; $1"
}

restart() {
  # The sudoers rule allows this exact argv only, so it cannot go through `bash -s`.
  # main checks the shape of UNIT, so the client-side expansion is safe.
  # shellcheck disable=SC2029
  ssh "$HOST" sudo systemctl restart "$UNIT" </dev/null
}

deploy_rev() {
  as_worker "git checkout -q --detach $1 && uv sync --frozen -q"
}

healthy() {
  local i
  for ((i = 0; i < HEALTH_TRIES; i++)); do
    if curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null; then
      return 0
    fi
    # The unit needs a few seconds to bind its port after a restart, and sends no signal.
    sleep "$HEALTH_DELAY"
  done
  return 1
}

running_jobs() {
  local body
  body="$(curl -fsS --max-time 5 "$HEALTH_URL")" || {
    echo 0
    return
  }
  jq -r '.running // 0' <<<"$body"
}

main() {
  local force=0
  if [[ "${1:-}" == "--force" ]]; then
    force=1
    shift
  fi
  [[ $# -ge 2 && $# -le 3 ]] || usage
  HOST="$1"
  HEALTH_URL="$2"
  local ref="${3:-origin/main}"
  # Shape only: no shell characters, and no leading "-" that git would read as an option.
  [[ "$ref" =~ ^[A-Za-z0-9._/-]+$ && "$ref" != -* ]] || usage
  # Both values reach a remote shell, so they must hold no quote or shell character.
  [[ "$UNIT" =~ ^[A-Za-z0-9@._-]+$ && "$APP_DIR" =~ ^/[A-Za-z0-9._/-]+$ ]] || {
    echo "deploy: ROBO_WORKER_UNIT or ROBO_WORKER_APP_DIR has an unsafe character" >&2
    exit 2
  }

  local running
  running="$(running_jobs)"
  if [[ "$running" != "0" && "$force" -eq 0 ]]; then
    echo "deploy: $running job(s) running. A restart interrupts them. Wait, or use --force." >&2
    exit 5
  fi

  local old
  old="$(as_worker 'git rev-parse HEAD')"
  [[ "$old" =~ ^[0-9a-f]{40}$ ]] || {
    echo "deploy: cannot read the current commit on $HOST (got '$old')" >&2
    exit 4
  }
  as_worker "git fetch -q origin"

  if deploy_rev "$ref" && restart && healthy; then
    echo "deploy: $ref is live on $HOST"
    exit 0
  fi
  echo "deploy: $ref failed on $HOST. Rolling back to $old." >&2
  if deploy_rev "$old" && restart && healthy; then
    exit 3
  fi
  echo "deploy: the rollback failed. $HOST needs a manual fix." >&2
  exit 4
}

main "$@"
