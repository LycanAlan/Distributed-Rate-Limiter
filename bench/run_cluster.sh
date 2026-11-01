#!/usr/bin/env bash
# Local cluster for the benchmarks (plan Task C8):
#   Redis :6379 (the system service) and :6380 (started here),
#   upstream :9000, gateways :8081-8083, all pointed at both Redis shards.
#
# Usage: bench/run_cluster.sh up <mode> [rules_file]
#        bench/run_cluster.sh down
#        bench/run_cluster.sh shard-down | shard-up    (stop or start :6380 only, for chaos.py)
# Env:   BUILD_DIR (default ~/rl-build), LUA_DIR (default: the gateway's compiled-in lua/)
set -euo pipefail

BENCH_DIR=$(cd "$(dirname "$0")" && pwd)
BUILD_DIR=${BUILD_DIR:-$HOME/rl-build}
GATEWAY_BIN=$BUILD_DIR/gateway
STATE_DIR=/tmp/rl
UPSTREAM_PORT=9000
GATEWAY_PORTS=(8081 8082 8083)
SYSTEM_REDIS_PORT=6379
EXTRA_REDIS_PORT=6380
STARTUP_TRIES=50  # x 0.1 s

die() {
  echo "run_cluster: $*" >&2
  exit 1
}

wait_for_http() {
  for _ in $(seq $STARTUP_TRIES); do
    curl -s -o /dev/null "http://127.0.0.1:$1/healthz" && return
    sleep 0.1
  done
  die "nothing answering on :$1 (see $STATE_DIR/*.log)"
}

wait_for_redis() {
  for _ in $(seq $STARTUP_TRIES); do
    redis-cli -p "$1" ping >/dev/null 2>&1 && return
    sleep 0.1
  done
  die "redis :$1 not answering"
}

# Waits for a process to exit, so its port is really free before we rebind it.
# (SO_REUSEPORT would otherwise let a dying old gateway share the port with a new one.)
wait_for_exit() {
  for _ in $(seq $STARTUP_TRIES); do
    kill -0 "$1" 2>/dev/null || return 0
    sleep 0.1
  done
  kill -9 "$1" 2>/dev/null || true
}

start_redis() {
  redis-cli -p $SYSTEM_REDIS_PORT ping >/dev/null 2>&1 ||
    die "system Redis on :$SYSTEM_REDIS_PORT is down (start it: service redis-server start)"
  if ! redis-cli -p $EXTRA_REDIS_PORT ping >/dev/null 2>&1; then
    redis-server --port $EXTRA_REDIS_PORT --save "" --appendonly no --daemonize yes \
      --pidfile "$STATE_DIR/redis-$EXTRA_REDIS_PORT.pid" --logfile "$STATE_DIR/redis-$EXTRA_REDIS_PORT.log"
    wait_for_redis $EXTRA_REDIS_PORT
  fi
}

start_gateway_process() {  # <log name> <gateway args...>
  local name=$1
  shift
  "$GATEWAY_BIN" "$@" 2>"$STATE_DIR/$name.log" &
  echo $! >"$STATE_DIR/$name.pid"
}

up() {
  local mode=${1:?usage: run_cluster.sh up <local|redis|hybrid|naive> [rules_file]}
  local rules=${2:-$BENCH_DIR/rules.bench.conf}
  [[ -x $GATEWAY_BIN ]] || die "no gateway at $GATEWAY_BIN (build first, or set BUILD_DIR)"
  down quiet
  mkdir -p "$STATE_DIR"
  start_redis

  local lua_args=()
  [[ -n ${LUA_DIR:-} ]] && lua_args=(--lua-dir "$LUA_DIR")
  local shards="127.0.0.1:$SYSTEM_REDIS_PORT,127.0.0.1:$EXTRA_REDIS_PORT"

  start_gateway_process upstream --serve-upstream --port $UPSTREAM_PORT
  for port in "${GATEWAY_PORTS[@]}"; do
    start_gateway_process "gateway-$port" --port "$port" --upstream 127.0.0.1:$UPSTREAM_PORT \
      --rules "$rules" --mode "$mode" --redis "$shards" --nodes ${#GATEWAY_PORTS[@]} "${lua_args[@]}"
  done

  wait_for_http $UPSTREAM_PORT
  for port in "${GATEWAY_PORTS[@]}"; do wait_for_http "$port"; done
  echo "cluster up: mode=$mode gateways=${GATEWAY_PORTS[*]} redis=$shards logs=$STATE_DIR"
}

down() {
  local quiet=${1:-}
  for pidfile in "$STATE_DIR"/upstream.pid "$STATE_DIR"/gateway-*.pid; do
    [[ -f $pidfile ]] || continue
    local pid
    pid=$(cat "$pidfile")
    kill "$pid" 2>/dev/null || true
    wait_for_exit "$pid"
    rm -f "$pidfile"
  done
  redis-cli -p $EXTRA_REDIS_PORT shutdown nosave >/dev/null 2>&1 || true
  [[ -n $quiet ]] || echo "cluster down"
}

case ${1:-} in
  up) up "${@:2}" ;;
  down) down ;;
  shard-down) redis-cli -p $EXTRA_REDIS_PORT shutdown nosave >/dev/null 2>&1 || true ;;
  shard-up) start_redis ;;
  *) die "usage: run_cluster.sh up <mode> [rules_file] | down | shard-down | shard-up" ;;
esac
