#!/usr/bin/env bash
# Usage: scripts/deploy.sh [--force] [git-ref]
#
# Deploy robo-worker to the phaedrus namespace. Check <git-ref> (default origin/main) out
# into a deploy worktree, apply deploy/k8s from it, wait for the control-plane rollout, and
# check its /health. Undo the rollout to the revision that ran before when either fails.
# The manifests in <git-ref> name the image tag. --force deploys even when a job Job is
# active, or when the active jobs cannot be counted.
#
# Exit codes: 0 deployed, 2 usage, 3 rolled back and healthy, 4 rollback failed or unhealthy
# or nothing to roll back to, 5 refused (jobs active or count unknown), 6 nothing changed
# (cannot fetch, check out, or read the Deployment).
set -euo pipefail

APP_DIR="${ROBO_WORKER_APP_DIR:-$HOME/git/robo-worker}"
DEPLOY_DIR="${ROBO_WORKER_DEPLOY_DIR:-$HOME/.cache/robo-worker-deploy}"
NAMESPACE="${ROBO_WORKER_NAMESPACE:-phaedrus}"
HEALTH_TRIES="${ROBO_WORKER_HEALTH_TRIES:-30}"
HEALTH_DELAY="${ROBO_WORKER_HEALTH_DELAY:-2}"
DEPLOYMENT="deployment/robo-worker"
# The API server proxies to the Service, so this needs no LAN route and no port-forward.
HEALTH_URL="/api/v1/namespaces/$NAMESPACE/services/robo-worker:8765/proxy/health"
REVISION_KEY="deployment.kubernetes.io/revision"

usage() {
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

kc() {
  kubectl -n "$NAMESPACE" "$@"
}

# Count the active job Jobs through the API server, not through /health: an unhealthy
# control plane must not hide a running job.
running_jobs() {
  local body
  body="$(kc get jobs -l robo-worker/job-id -o json)" || return 1
  jq '[.items[] | select((.status.active // 0) > 0)] | length' <<<"$body"
}

# Print the revision of the Deployment, or nothing when it does not exist yet.
current_revision() {
  local body
  body="$(kc get "$DEPLOYMENT" --ignore-not-found -o json)" || return 1
  [[ -n "$body" ]] || return 0
  jq -r --arg key "$REVISION_KEY" '.metadata.annotations[$key] // ""' <<<"$body"
}

checkout_ref() {
  git -C "$APP_DIR" fetch -q origin || return 1
  if [[ -e "$DEPLOY_DIR/.git" ]]; then
    git -C "$DEPLOY_DIR" checkout -q --detach "$1"
  else
    mkdir -p "$(dirname "$DEPLOY_DIR")"
    git -C "$APP_DIR" worktree add -q --detach "$DEPLOY_DIR" "$1"
  fi
}

healthy() {
  local i body="" err
  err="$(mktemp)"
  for ((i = 0; i < HEALTH_TRIES; i++)); do
    if body="$(kc get --raw "$HEALTH_URL" 2>"$err")" &&
      jq -e '.ok == true' <<<"$body" >/dev/null; then
      rm -f "$err"
      return 0
    fi
    # The new pod needs a few seconds to bind its port, and sends no signal.
    sleep "$HEALTH_DELAY"
  done
  echo "deploy: /health of the robo-worker Service is not ok." >&2
  jq -r '.problems[]? | "deploy:   problem: \(.)"' <<<"$body" >&2 2>/dev/null || true
  if [[ -s "$err" ]]; then
    echo "deploy:   kubectl: $(tail -n 1 "$err")" >&2
  fi
  rm -f "$err"
  return 1
}

apply_and_wait() {
  kc apply -k "$DEPLOY_DIR/deploy/k8s" >/dev/null &&
    kc rollout status "$DEPLOYMENT" --timeout=300s >/dev/null &&
    healthy
}

roll_back() {
  local old="$1" now
  if [[ -z "$old" ]]; then
    echo "deploy: this was the first deploy, so no revision exists to roll back to." >&2
    return 1
  fi
  now="$(current_revision)" || return 1
  if [[ "$now" == "$old" ]]; then
    # The Deployment did not change. An undo here would go back one more revision.
    echo "deploy: the Deployment is still at revision $old, so it needs no undo." >&2
  else
    kc rollout undo "$DEPLOYMENT" --to-revision="$old" >/dev/null &&
      kc rollout status "$DEPLOYMENT" --timeout=300s >/dev/null || return 1
  fi
  # An undo reverts the Deployment only. The other objects from the ref stay applied.
  echo "deploy: other objects from the ref (Role, Service, claims, ConfigMap) stay applied." >&2
  healthy
}

refuse_if_busy() {
  local force="$1" running
  if ! running="$(running_jobs)"; then
    if [[ "$force" -eq 0 ]]; then
      echo "deploy: cannot count the active jobs in $NAMESPACE." \
        "Fix kubectl access, or use --force." >&2
      exit 5
    fi
    echo "deploy: warning: cannot count the active jobs, and --force deploys anyway." >&2
    return 0
  fi
  if [[ "$running" != "0" && "$force" -eq 0 ]]; then
    echo "deploy: $running job(s) active. A rollout stops none of them, but a config" \
      "change can change how the control plane reads them. Wait, or use --force." >&2
    exit 5
  fi
}

main() {
  local force=0
  if [[ "${1:-}" == "--force" ]]; then
    force=1
    shift
  fi
  [[ $# -le 1 ]] || usage
  local ref="${1:-origin/main}"
  # Shape only: no shell characters, and no leading "-" that git would read as an option.
  [[ "$ref" =~ ^[A-Za-z0-9._/-]+$ && "$ref" != -* ]] || usage

  refuse_if_busy "$force"
  local old
  old="$(current_revision)" || {
    echo "deploy: cannot read $DEPLOYMENT in $NAMESPACE. Nothing changed." >&2
    exit 6
  }
  checkout_ref "$ref" || {
    echo "deploy: cannot fetch or check out $ref in $APP_DIR. Nothing changed." >&2
    exit 6
  }

  if apply_and_wait; then
    echo "deploy: $ref is live in the $NAMESPACE namespace"
    exit 0
  fi
  echo "deploy: $ref failed. Rolling back." >&2
  if roll_back "$old"; then
    echo "deploy: rolled back to revision $old, and it is healthy." >&2
    exit 3
  fi
  echo "deploy: the rollback failed, or the old revision is not healthy." \
    "The cluster needs a manual fix." >&2
  exit 4
}

main "$@"
