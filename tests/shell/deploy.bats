#!/usr/bin/env bats

OLD_SHA=0123456789abcdef0123456789abcdef01234567

setup() {
  BIN="$BATS_TEST_TMPDIR/bin"
  mkdir -p "$BIN"
  export LOG="$BATS_TEST_TMPDIR/log" COUNT="$BATS_TEST_TMPDIR/curl-count" OLD_SHA
  cat >"$BIN/ssh" <<'EOF'
#!/usr/bin/env bash
input="$(cat)"
echo "ssh $* :: $input" >>"$LOG"
if [[ "$input" == *"rev-parse HEAD"* ]]; then echo "$OLD_SHA"; fi
if [[ -n "${FAIL_SYNC:-}" && "$input" == *"uv sync"* && "$input" != *"$OLD_SHA"* ]]; then
  exit 1
fi
exit 0
EOF
  cat >"$BIN/curl" <<'EOF'
#!/usr/bin/env bash
n=$(($(cat "$COUNT" 2>/dev/null || echo 0) + 1))
echo "$n" >"$COUNT"
echo "curl $n $*" >>"$LOG"
if [[ "$n" -eq 1 ]]; then echo "{\"running\": ${RUNNING:-0}}"; exit 0; fi
if [[ "$n" -le "${HEALTH_FAIL_UNTIL:-1}" ]]; then exit 22; fi
echo '{"ok": true}'
EOF
  chmod +x "$BIN/ssh" "$BIN/curl"
  export PATH="$BIN:$PATH" ROBO_WORKER_HEALTH_TRIES=2 ROBO_WORKER_HEALTH_DELAY=0
}

@test "usage error exits 2" {
  run scripts/deploy.sh
  [ "$status" -eq 2 ]
}

@test "a ref with shell characters exits 2" {
  run scripts/deploy.sh host http://h/health 'main;reboot'
  [ "$status" -eq 2 ]
}

@test "refuses to restart while jobs run" {
  RUNNING=2 run scripts/deploy.sh host http://h/health
  [ "$status" -eq 5 ]
  ! grep -q systemctl "$LOG"
}

@test "--force deploys while jobs run" {
  RUNNING=2 run scripts/deploy.sh --force host http://h/health
  [ "$status" -eq 0 ]
}

@test "deploys the ref and restarts the unit" {
  run scripts/deploy.sh host http://h/health v0.1.0
  [ "$status" -eq 0 ]
  grep -q "git checkout -q --detach v0.1.0" "$LOG"
  grep -q "systemctl restart robo-worker" "$LOG"
}

@test "rolls back when the health check fails" {
  HEALTH_FAIL_UNTIL=3 run scripts/deploy.sh host http://h/health v0.1.0
  [ "$status" -eq 3 ]
  grep -q "detach $OLD_SHA" "$LOG"
}

@test "rolls back when uv sync fails" {
  FAIL_SYNC=1 run scripts/deploy.sh host http://h/health v0.1.0
  [ "$status" -eq 3 ]
  grep -q "detach $OLD_SHA" "$LOG"
}

@test "exit 4 when the rollback fails too" {
  HEALTH_FAIL_UNTIL=99 run scripts/deploy.sh host http://h/health v0.1.0
  [ "$status" -eq 4 ]
}

@test "a ref that starts with a dash exits 2" {
  run scripts/deploy.sh host http://h/health -h
  [ "$status" -eq 2 ]
}

@test "a unit name with shell characters exits 2" {
  ROBO_WORKER_UNIT='robo-worker;reboot' run scripts/deploy.sh host http://h/health
  [ "$status" -eq 2 ]
  [ ! -e "$LOG" ]
}
