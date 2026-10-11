#!/usr/bin/env bash
# Usage: scripts/deploy.sh [--force] <ssh-host> <health-url> [git-ref]
#
# Deploy robo-worker on <ssh-host>: check out <git-ref> (default origin/main), run uv sync,
# restart the unit, and check <health-url>. Revert to the previous commit when that fails.
# --force deploys even when jobs run, or when the running job count cannot be read;
# the restart interrupts the jobs, and they resume later.
#
# Exit codes: 0 deployed, 2 usage, 3 rolled back, 4 rollback failed, 5 refused (jobs running
# or count unknown), 6 cannot read or fetch the checkout on the host (nothing changed).
set -euo pipefail

APP_DIR="${ROBO_WORKER_APP_DIR:-/home/robo-worker/robo-worker}"
UNIT="${ROBO_WORKER_UNIT:-robo-worker}"
HEALTH_TRIES="${ROBO_WORKER_HEALTH_TRIES:-15}"
HEALTH_DELAY="${ROBO_WORKER_HEALTH_DELAY:-2}"

usage() {
  sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//' >&2
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

# Set HEALTH_CODE and HEALTH_BODY. /health answers 503 with a JSON body when a job cannot
# run, so the body is read on both codes. Return 1 only when there is no answer at all.
health_get() {
  local out
  HEALTH_CODE="" HEALTH_BODY=""
  out="$(curl -sS --max-time 5 -w '\n%{http_code}' "$HEALTH_URL")" || return 1
  HEALTH_CODE="${out##*$'\n'}"
  HEALTH_BODY="${out%$'\n'*}"
}

healthy() {
  local i
  for ((i = 0; i < HEALTH_TRIES; i++)); do
    if health_get && [[ "$HEALTH_CODE" == "200" ]]; then
      return 0
    fi
    # The unit needs a few seconds to bind its port after a restart, and sends no signal.
    sleep "$HEALTH_DELAY"
  done
  echo "deploy: $HEALTH_URL is not healthy (HTTP ${HEALTH_CODE:-none})." >&2
  jq -r '.problems[]? | "deploy:   problem: \(.)"' <<<"$HEALTH_BODY" >&2 2>/dev/null || true
  return 1
}

# Print the running job count, or "unknown" when the service gives none.
running_jobs() {
  if ! health_get || [[ "$HEALTH_CODE" != "200" && "$HEALTH_CODE" != "503" ]]; then
    echo unknown
    return
  fi
  jq -r 'if (.running | type) == "number" then .running else "unknown" end' \
    <<<"$HEALTH_BODY" 2>/dev/null || echo unknown
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
  # ssh and curl read a leading "-" as an option.
  [[ "$HOST" =~ ^[A-Za-z0-9][A-Za-z0-9._@-]*$ ]] || usage
  [[ "$HEALTH_URL" =~ ^https?://[^[:space:]]+$ ]] || usage
  # Both values reach a remote shell, so they must hold no quote or shell character.
  [[ "$UNIT" =~ ^[A-Za-z0-9@._-]+$ && "$APP_DIR" =~ ^/[A-Za-z0-9._/-]+$ ]] || {
    echo "deploy: ROBO_WORKER_UNIT or ROBO_WORKER_APP_DIR has an unsafe character" >&2
    exit 2
  }

  local running
  running="$(running_jobs)"
  if [[ "$running" == "unknown" && "$force" -eq 0 ]]; then
    echo "deploy: cannot read the running job count from $HEALTH_URL." \
      "Check the service, or use --force." >&2
    exit 5
  fi
  if [[ "$running" != "0" && "$running" != "unknown" && "$force" -eq 0 ]]; then
    echo "deploy: $running job(s) running. A restart interrupts them. Wait, or use --force." >&2
    exit 5
  fi

  local old=""
  old="$(as_worker 'git rev-parse HEAD')" || true
  [[ "$old" =~ ^[0-9a-f]{40}$ ]] || {
    echo "deploy: cannot read the current commit on $HOST (got '$old'). Nothing changed." >&2
    exit 6
  }
  as_worker "git fetch -q origin" || {
    echo "deploy: git fetch failed on $HOST. Nothing changed." >&2
    exit 6
  }

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
