#!/usr/bin/env bash
# Throughput and latency: the upstream directly vs through one gateway node in
# each mode (plan Task C10). /overhead has a limit of 1e9, so nothing is denied
# and this measures the cost of the gateway, not of limiting. One extra run hits
# /deny (keyed by IP, limit 1), where every request after the first gets a 429.
#
# Prints a markdown table. Each target is measured RUNS times; the run with the
# median req/s is reported.
#
# Usage: bench/wrk.sh [duration] [runs]   (default 15s 3; BUILD_DIR as for run_cluster.sh)
set -euo pipefail

BENCH_DIR=$(cd "$(dirname "$0")" && pwd)
DURATION=${1:-15s}
RUNS=${2:-3}
WRK=(wrk -t4 -c64 -d"$DURATION" --latency -s "$BENCH_DIR/random_user.lua")
GATEWAY=http://127.0.0.1:8081
UPSTREAM=http://127.0.0.1:9000

trap '"$BENCH_DIR/run_cluster.sh" down >/dev/null' EXIT

# One wrk run summarised as "req/s p50 p99".
one_run() {
  "${WRK[@]}" "$1" | awk '
    /^Requests\/sec/ { rps = $2 }
    $1 == "50%"      { p50 = $2 }
    $1 == "99%"      { p99 = $2 }
    END              { printf "%.0f %s %s\n", rps, p50, p99 }'
}

# Prints a table row for the median-throughput run of RUNS.
measure() {  # <label> <url>
  local runs=()
  for _ in $(seq "$RUNS"); do runs+=("$(one_run "$2")"); done
  local median
  median=$(printf '%s\n' "${runs[@]}" | sort -n | sed -n "$(((RUNS + 1) / 2))p")
  read -r rps p50 p99 <<<"$median"
  echo "| $1 | $rps | $p50 | $p99 |"
}

echo "wrk -t4 -c64 -d$DURATION, median of $RUNS runs, one gateway node"
echo
echo "| target | req/s | p50 | p99 |"
echo "|---|---|---|---|"
"$BENCH_DIR/run_cluster.sh" up local >/dev/null
measure "upstream directly (no gateway)" "$UPSTREAM/overhead"
measure "gateway, local mode" "$GATEWAY/overhead"
for mode in redis hybrid; do
  "$BENCH_DIR/run_cluster.sh" up "$mode" >/dev/null
  measure "gateway, $mode mode" "$GATEWAY/overhead"
done
measure "gateway, hybrid mode, every request denied" "$GATEWAY/deny"
"$BENCH_DIR/run_cluster.sh" up redis >/dev/null
measure "gateway, redis mode, every request denied" "$GATEWAY/deny"
