#!/usr/bin/env bats

bats_require_minimum_version 1.5.0

REF=v0.1.0

setup() {
  BIN="$BATS_TEST_TMPDIR/bin"
  mkdir -p "$BIN"
  export LOG="$BATS_TEST_TMPDIR/kubectl-log" GIT_LOG="$BATS_TEST_TMPDIR/git-log"
  export REV_FILE="$BATS_TEST_TMPDIR/revision" REF
  echo "${START_REV-3}" >"$REV_FILE"
  cat >"$BIN/kubectl" <<'EOF'
#!/usr/bin/env bash
echo "kubectl $*" >>"$LOG"
shift 2 # -n <namespace>
case "$1 $2" in
"get --raw")
  if [[ -n "${HEALTH_FORBIDDEN:-}" ]]; then
    echo 'Error from server (Forbidden): services "robo-worker" is forbidden' >&2
    exit 1
  fi
  ok="${HEALTH_OK:-true}"
  # After an undo, HEALTH_AFTER_UNDO decides the answer.
  if [[ "$(cat "$REV_FILE")" == "5" && -n "${HEALTH_AFTER_UNDO:-}" ]]; then
    ok="$HEALTH_AFTER_UNDO"
  fi
  if [[ "$ok" == "true" ]]; then echo '{"ok":true}'; else
    echo '{"ok":false,"problems":["token expired"]}'
  fi
  ;;
"get jobs")
  [[ -z "${JOBS_EXIT:-}" ]] || exit "$JOBS_EXIT"
  # One finished Job always, so the count must skip it.
  items='{"status":{"succeeded":1}}'
  for ((i = 0; i < ${RUNNING:-0}; i++)); do items+=',{"status":{"active":1}}'; done
  echo "{\"items\":[${items}]}"
  ;;
"get deployment/robo-worker")
  [[ -z "${GET_DEPLOY_EXIT:-}" ]] || exit "$GET_DEPLOY_EXIT"
  rev="$(cat "$REV_FILE")"
  [[ -z "$rev" ]] && exit 0 # --ignore-not-found prints nothing
  echo "{\"metadata\":{\"annotations\":{\"deployment.kubernetes.io/revision\":\"$rev\"}}}"
  ;;
"apply -k")
  [[ -z "${APPLY_EXIT:-}" ]] || exit "$APPLY_EXIT"
  echo 4 >"$REV_FILE"
  ;;
"rollout status") exit "${ROLLOUT_EXIT:-0}" ;;
"rollout undo")
  [[ -z "${UNDO_EXIT:-}" ]] || exit "$UNDO_EXIT"
  echo 5 >"$REV_FILE"
  ;;
*) exit 0 ;;
esac
EOF
  cat >"$BIN/git" <<'EOF'
#!/usr/bin/env bash
echo "git $*" >>"$GIT_LOG"
if [[ -n "${FAIL_FETCH:-}" && "$*" == *" fetch "* ]]; then exit 1; fi
exit 0
EOF
  chmod +x "$BIN/kubectl" "$BIN/git"
  export PATH="$BIN:$PATH" ROBO_WORKER_HEALTH_TRIES=2 ROBO_WORKER_HEALTH_DELAY=0
  export ROBO_WORKER_APP_DIR="$BATS_TEST_TMPDIR/app"
  export ROBO_WORKER_DEPLOY_DIR="$BATS_TEST_TMPDIR/deploy"
}

@test "usage error exits 2" {
  run scripts/deploy.sh a b
  [ "$status" -eq 2 ]
  [ ! -e "$LOG" ]
}

@test "a ref with shell characters exits 2" {
  run scripts/deploy.sh 'main;reboot'
  [ "$status" -eq 2 ]
  [ ! -e "$LOG" ]
}

@test "a ref that starts with a dash exits 2" {
  run scripts/deploy.sh -h
  [ "$status" -eq 2 ]
}

@test "refuses to deploy while jobs run" {
  RUNNING=2 run scripts/deploy.sh "$REF"
  [ "$status" -eq 5 ]
  [[ "$output" == *"2 job(s) active"* ]]
  run ! grep -q "apply" "$LOG"
}

@test "refuses to deploy when it cannot count the active jobs" {
  JOBS_EXIT=1 run scripts/deploy.sh "$REF"
  [ "$status" -eq 5 ]
  run ! grep -q "apply" "$LOG"
}

@test "--force deploys while jobs run" {
  RUNNING=2 run scripts/deploy.sh --force "$REF"
  [ "$status" -eq 0 ]
}

@test "--force deploys when it cannot count the active jobs" {
  JOBS_EXIT=1 run scripts/deploy.sh --force "$REF"
  [ "$status" -eq 0 ]
}

@test "deploys the ref and applies the manifests" {
  run scripts/deploy.sh "$REF"
  [ "$status" -eq 0 ]
  grep -q "worktree add -q --detach $ROBO_WORKER_DEPLOY_DIR $REF" "$GIT_LOG"
  grep -q "apply -k $ROBO_WORKER_DEPLOY_DIR/deploy/k8s" "$LOG"
  grep -q "rollout status deployment/robo-worker" "$LOG"
}

@test "a later deploy reuses the deploy worktree" {
  mkdir -p "$ROBO_WORKER_DEPLOY_DIR/.git"
  run scripts/deploy.sh "$REF"
  [ "$status" -eq 0 ]
  grep -q "checkout -q --detach $REF" "$GIT_LOG"
  run ! grep -q "worktree add" "$GIT_LOG"
}

@test "rolls back to the old revision when the service is not healthy" {
  HEALTH_OK=false HEALTH_AFTER_UNDO=true run scripts/deploy.sh "$REF"
  [ "$status" -eq 3 ]
  grep -q "rollout undo deployment/robo-worker --to-revision=3" "$LOG"
  [[ "$output" == *"problem: token expired"* ]]
}

@test "an apply that fails before the Deployment changes undoes nothing" {
  APPLY_EXIT=1 run scripts/deploy.sh "$REF"
  [ "$status" -eq 3 ]
  [[ "$output" == *"stay applied"* ]]
  [[ "$output" == *"still at revision 3"* ]]
  # run replaces $output, so the checks of the script's output come first.
  run ! grep -q "rollout undo" "$LOG"
}

@test "exit 4 when the rollback fails too" {
  HEALTH_OK=false UNDO_EXIT=1 run scripts/deploy.sh "$REF"
  [ "$status" -eq 4 ]
}

@test "exit 4 when the first deploy fails, with nothing to roll back to" {
  START_REV="" setup
  HEALTH_OK=false run scripts/deploy.sh "$REF"
  [ "$status" -eq 4 ]
  [[ "$output" == *"no revision exists"* ]]
  run ! grep -q "rollout undo" "$LOG"
}

@test "exit 6 when git fetch fails" {
  FAIL_FETCH=1 run scripts/deploy.sh "$REF"
  [ "$status" -eq 6 ]
  run ! grep -q "apply" "$LOG"
}

@test "exit 6 when the Deployment cannot be read" {
  GET_DEPLOY_EXIT=1 run scripts/deploy.sh "$REF"
  [ "$status" -eq 6 ]
  run ! grep -q "apply" "$LOG"
}

@test "exit 4 when the old revision is not healthy after the undo" {
  HEALTH_OK=false HEALTH_AFTER_UNDO=false run scripts/deploy.sh "$REF"
  [ "$status" -eq 4 ]
  grep -q "rollout undo deployment/robo-worker --to-revision=3" "$LOG"
}

@test "a failed health check prints the kubectl error" {
  HEALTH_FORBIDDEN=1 run scripts/deploy.sh "$REF"
  [ "$status" -eq 4 ]
  [[ "$output" == *"kubectl: Error from server (Forbidden)"* ]]
}

@test "--force warns when it cannot count the active jobs" {
  JOBS_EXIT=1 run scripts/deploy.sh --force "$REF"
  [ "$status" -eq 0 ]
  [[ "$output" == *"warning: cannot count the active jobs"* ]]
}
