#!/usr/bin/env bash
# End-to-end check of one gateway (plan Tasks C6, C8).
# Starts a dummy upstream and one gateway on spare ports, then checks real
# HTTP behaviour with curl and raw sockets.
#
# Usage: [MODE=local|redis|naive|hybrid] [LUA_DIR=...] bench/smoke.sh [build_dir]   (default: ~/rl-build)
# The Redis modes use the Redis on :6379.
set -euo pipefail

MODE=${MODE:-local}
BUILD_DIR=${1:-$HOME/rl-build}
GATEWAY_BIN=$BUILD_DIR/gateway
GATEWAY_PORT=18081
UPSTREAM_PORT=19000
BASE=http://127.0.0.1:$GATEWAY_PORT
WORK_DIR=$(mktemp -d)

cleanup() {
  kill "${UPSTREAM_PID:-}" "${GATEWAY_PID:-}" 2>/dev/null || true
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*"
  echo "--- gateway log"; cat "$WORK_DIR/gateway.log" || true
  exit 1
}
pass() { echo "ok    $*"; }

wait_until_up() {
  for _ in $(seq 50); do
    curl -s -o /dev/null "http://127.0.0.1:$1/healthz" && return
    sleep 0.1
  done
  fail "nothing listening on :$1"
}

status_of() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

expect_status() {
  local want=$1
  shift
  local got
  got=$(status_of "$@")
  [[ $got == "$want" ]] || fail "$* returned $got, expected $want"
  pass "$want  $*"
}

# Sends raw bytes on one TCP connection and prints everything the server sends back.
raw_exchange() {
  exec 3<>"/dev/tcp/127.0.0.1/$GATEWAY_PORT"
  printf '%b' "$1" >&3
  timeout 2 cat <&3 || true
  exec 3<&-
}

[[ -x $GATEWAY_BIN ]] || fail "no gateway binary at $GATEWAY_BIN (build first)"
# A rule name unique to this run, so Redis counts from an earlier run don't leak in.
echo "smoke_$$ * /limited ip fixed_window 3 60000 closed" >"$WORK_DIR/rules.conf"
lua_args=()
[[ -n ${LUA_DIR:-} ]] && lua_args=(--lua-dir "$LUA_DIR")

"$GATEWAY_BIN" --serve-upstream --port $UPSTREAM_PORT 2>"$WORK_DIR/upstream.log" &
UPSTREAM_PID=$!
"$GATEWAY_BIN" --port $GATEWAY_PORT --upstream 127.0.0.1:$UPSTREAM_PORT --rules "$WORK_DIR/rules.conf" \
  --mode "$MODE" --redis 127.0.0.1:6379 "${lua_args[@]}" 2>"$WORK_DIR/gateway.log" &
GATEWAY_PID=$!
wait_until_up $UPSTREAM_PORT
wait_until_up $GATEWAY_PORT

# 1. The limit: 3 allowed, then 429 with the right headers.
headers=$(curl -s -D - -o /dev/null "$BASE/limited")
grep -q "^HTTP/1.1 200" <<<"$headers" || fail "first /limited not 200"
# In hybrid mode Remaining is what this node has leased, not the global count.
if [[ $MODE != hybrid ]]; then
  grep -qi "^X-RateLimit-Remaining: 2" <<<"$headers" || fail "missing X-RateLimit-Remaining: 2"
fi
pass "200  first /limited"
expect_status 200 "$BASE/limited"
expect_status 200 "$BASE/limited"
headers=$(curl -s -D - -o /dev/null "$BASE/limited")
grep -q "^HTTP/1.1 429" <<<"$headers" || fail "4th /limited not 429"
grep -qi "^Retry-After: [1-9]" <<<"$headers" || fail "429 without a positive Retry-After"
pass "429  4th /limited, Retry-After present"

# 2. No rule: proxied straight through.
expect_status 200 "$BASE/free"

# 3. Keep-alive: curl reuses one connection for two requests.
reuse=$(curl -sv -o /dev/null -o /dev/null "$BASE/free" "$BASE/free" 2>&1 | grep -ci "re-using existing connection" || true)
[[ $reuse -ge 1 ]] || fail "second request did not reuse the connection"
pass "keep-alive connection reused"

# 4. Pipelining: two requests in one write, two responses back in order.
responses=$(raw_exchange 'GET /free HTTP/1.1\r\nHost: x\r\n\r\nGET /free HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n' | grep -c "^HTTP/1.1 200" || true)
[[ $responses == 2 ]] || fail "pipelined requests got $responses responses, expected 2"
pass "pipelined requests both answered"

# 5. Malformed request: 400 and the connection closes.
first_line=$(raw_exchange 'HELLO\r\n\r\n' | head -1 | tr -d '\r')
[[ $first_line == "HTTP/1.1 400 Bad Request" ]] || fail "malformed request got '$first_line'"
pass "400  malformed request"

# 6. Metrics reflect what happened.
metrics=$(curl -s "$BASE/metrics")
grep -q 'rl_requests_total{decision="allowed"} 3' <<<"$metrics" || fail "metrics: allowed != 3"
grep -q 'rl_requests_total{decision="denied"} 1' <<<"$metrics" || fail "metrics: denied != 1"
grep -q 'rl_decision_latency_us_count 4' <<<"$metrics" || fail "metrics: latency count != 4"
pass "metrics: 3 allowed, 1 denied, 4 latency samples"

# 7. Upstream down: 502, not a hang or a crash.
kill $UPSTREAM_PID
wait $UPSTREAM_PID 2>/dev/null || true
expect_status 502 "$BASE/free"
expect_status 200 "$BASE/healthz"

# 8. SIGTERM: graceful exit with status 0.
kill -TERM $GATEWAY_PID
wait $GATEWAY_PID
pass "gateway exited cleanly on SIGTERM"
GATEWAY_PID=

echo "smoke ($MODE mode): all checks passed"
