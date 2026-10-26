# Distributed Rate Limiter: Design Spec

Date: 2026-10-26
Status: approved (part 1 in chat; parts 2-3 reviewed with the plan)

## 1. Goal

An HTTP gateway in C++17 that sits in front of an upstream service and enforces
rate limits ("100 req/min per user") **consistently across 3 gateway nodes**,
using Redis as shared state, with measured accuracy, latency and failure behaviour.

Success criteria (the numbers that go on the resume):
- Throughput and p50/p99 added latency of the gateway vs hitting upstream directly (wrk).
- Over-admission % across 3 nodes: naive GET/SET vs atomic Lua vs hybrid leasing.
- Key movement when a Redis shard is added (consistent hashing), target ~1/N.
- Behaviour when a Redis shard dies: breaker open time, fallback decisions, recovery time.

Non-goals: TLS, chunked transfer encoding, HTTP/2, config hot reload, Redis Sentinel,
Docker, dashboards, leaky bucket (theory only).

## 2. Platform and stack

- Linux only (WSL2 Ubuntu 24.04). epoll, timers, `SO_REUSEPORT`.
- C++17, g++ 13, CMake + Ninja.
- hiredis 1.2 (async API with a custom epoll adapter). Redis 7.0 (scripts may call `TIME`).
- GoogleTest for unit and Redis integration tests.
- wrk for throughput/latency. Python 3.12 stdlib only for accuracy/race/chaos scripts.

## 3. Architecture

```
client ──HTTP──► gateway node (x3, :8081-8083)
                  │ epoll loop (single thread per process)
                  │ ├─ HTTP parser → rule match → client key (ip | header)
                  │ ├─ decision mode: local | redis | hybrid | naive
                  │ │     redis call → HashRing → shard (:6379, :6380)
                  │ │                  └─ CircuitBreaker per shard
                  │ ├─ allowed → proxy to upstream (pooled keep-alive conns)
                  │ └─ denied  → 429 + Retry-After + X-RateLimit-* headers
                  └─ GET /metrics (Prometheus text), GET /healthz
upstream: same binary with --serve-upstream, replies 200 "ok"
```

Single-threaded reactor per process (nginx/Redis model). No locks on the request
path. Scale by running more processes. Redis calls are async; a request is a
continuation resumed when Redis replies or times out.

## 4. Ownership

| Unit | Owner | File(s) |
|---|---|---|
| Event loop (epoll + timer heap) | Claude | `src/event_loop.{h,cpp}` |
| HTTP/1.1 parser + response writer | Claude | `src/http.{h,cpp}` |
| Server, rule matching, proxy, upstream mode | Claude | `src/server.{h,cpp}`, `src/upstream_pool.{h,cpp}` |
| Async Redis (hiredis epoll adapter, EVALSHA, timeouts) | Claude | `src/redis_async.{h,cpp}` |
| Config, metrics, main | Claude | `src/config.{h,cpp}`, `src/metrics.{h,cpp}`, `src/main.cpp` |
| Hybrid lease cache, fallback wiring | Claude | `src/decider.{h,cpp}` |
| **Local limiters (4 algorithms)** | **Ali** | `src/limiters.cpp` (header given) |
| **Lua scripts (4 algorithms + lease)** | **Ali** | `lua/*.lua` |
| **Consistent hash ring** | **Ali** | `src/hash_ring.cpp` (header given) |
| **Circuit breaker** | **Ali** | `src/circuit_breaker.cpp` (header given) |
| Benchmarks | Claude | `bench/*` |

Claude writes the header and failing tests for every unit Ali owns. Ali's unit is
done when its tests are green. Claude checks each test file against a throwaway
reference implementation first, so a red test always means the student code,
never a bad test.

## 5. Contracts for Ali's units

### 5.1 Decision (shared)
```cpp
struct Decision {
  bool allowed;
  int64_t remaining;       // requests still allowed right now, >= 0
  int64_t retry_after_ms;  // 0 if allowed; else ms until a request could pass
};
```

### 5.2 Local limiters (`now_ms` is passed in, so tests use a fake clock)
All take `(limit, window_ms)`. Per-key state lives in an `unordered_map`.
- **FixedWindow**: window = `now_ms / window_ms` (epoch-aligned). Allow if count < limit.
  Denied → `retry_after_ms = window_end - now_ms`.
- **SlidingLog**: keep timestamps `> now_ms - window_ms`. Allow if size < limit.
  Denied → `retry_after_ms = oldest + window_ms - now_ms`.
- **SlidingWindowCounter**: `estimate = prev * (window_ms - elapsed) / window_ms + curr`.
  Allow if `estimate < limit` (integer form: `prev*(window_ms-elapsed) + curr*window_ms < limit*window_ms`).
  Denied → `0 < retry_after_ms <= window_ms + 1`.
- **TokenBucket**: capacity = limit, refill rate = `limit / window_ms` tokens per ms,
  starts full. Allow if tokens >= 1. `remaining = floor(tokens)` after the take.
  Denied → `retry_after_ms = ceil((1 - tokens) / rate)`.

### 5.3 Lua scripts
`EVALSHA sha 1 <key> <limit> <window_ms> [chunk]`. Time comes from `redis.call('TIME')`,
never from the gateway (no clock skew between nodes). Every key gets a `PEXPIRE` so
memory stays bounded. Return `{allowed(0|1), remaining, retry_after_ms}`.
- `fixed_window.lua`, `sliding_log.lua` (ZSET, unique members via an `INCR` sequence),
  `sliding_counter.lua`, `token_bucket.lua` (HASH of tokens + ts).
- `lease.lua` (hybrid): fixed-window quota. ARGV[3] = chunk.
  `grant = min(chunk, limit - used)`, clamped at 0. Returns `{grant, ms_until_window_end}`.

### 5.4 HashRing
`HashRing(int vnodes = 100)`, `add_node`, `remove_node`, `node_for(key)` (throws
`std::runtime_error` on an empty ring), `size()`. Required properties: deterministic;
3 nodes × 100 vnodes over 30k keys, each node gets 20-47%; adding a 4th node moves
15-35% of keys, all of them to the new node; removing a node moves only its keys.

### 5.5 CircuitBreaker
`CircuitBreaker(int failure_threshold, int64_t open_ms)`.
- Closed: `allow_request` true. `on_failure` counts consecutive failures; at threshold → Open.
  `on_success` resets the count.
- Open: `allow_request` false until `open_ms` passed, then → HalfOpen and returns true once (the probe).
- HalfOpen: further `allow_request` false while the probe is out. `on_success` → Closed,
  `on_failure` → Open with a fresh timer.

## 6. Request flow (part 2)

1. Read and parse the request. `/metrics` and `/healthz` are served locally.
2. Match the first rule whose method (or `*`) and path prefix match. No rule → proxy straight through.
3. Client key = `rule.key_by`: `ip` or `header:<Name>`; a missing header falls back to the IP.
   Redis key = `rl:<rule>:<client key>`.
4. Decide using the mode set by `--mode`:
   - `local`: the in-process limiter. Not shared between nodes; the baseline for "wrong by N×".
   - `redis`: shard = `ring.node_for(redis key)`. If the shard's breaker allows, send `EVALSHA`
     with a 50 ms timeout. Reply → `on_success`, use the decision. Error/timeout → `on_failure`, fallback.
     Breaker open → fallback without calling Redis.
   - `hybrid`: lease cache. Serve from a locally leased quota; when it hits 0, call `lease.lua`
     for a chunk (default `max(1, limit / (nodes * 4))`). Requests for the same key queue
     while a lease is in flight. A lease expires at window end.
   - `naive`: fixed window via `GET` then `SET count+1`, two round trips, not atomic.
     Exists only to show the race.
5. Fallback per rule (`fail` column): `open` allows, `closed` returns 429, `local` uses a local
   limiter with `limit / --nodes`.
6. Allowed → proxy to upstream with `X-RateLimit-Limit` and `X-RateLimit-Remaining`.
   Denied → `429`, `Retry-After: ceil(retry_after_ms/1000)`, the same headers.

### Rules file (`rules.conf`, whitespace separated, `#` comments)
```
# name  method path_prefix key_by            algo             limit  window_ms  fail
login   POST   /login      ip                token_bucket     5      60000      closed
api     *      /api        header:X-User-Id  sliding_counter  100    60000      local
```
`algo` is one of `fixed_window`, `sliding_log`, `sliding_counter` or `token_bucket`.

### CLI
```
gateway --port 8081 --upstream 127.0.0.1:9000 --rules rules.conf --mode redis \
        --redis 127.0.0.1:6379,127.0.0.1:6380 --nodes 3
gateway --serve-upstream --port 9000
```

## 7. Failure handling (part 3)

- Redis command timeout 50 ms (hiredis async timeout, driven by our timer heap).
- A circuit breaker per shard: threshold 5 consecutive failures, open for 2000 ms.
- Redis reconnect: on disconnect, retry every 500 ms. Commands fail fast while it's down.
- Upstream: 2 s connect/read timeout → 502 to the client. A connection that errors is dropped from the pool.
- Client: 10 s idle timeout on keep-alive connections. Malformed request → 400 and close.
  Headers > 16 KB → 431 and close.
- SIGPIPE ignored, SIGINT/SIGTERM → stop accepting, then exit after in-flight requests finish (max 2 s).

## 8. Metrics (`GET /metrics`)
`rl_requests_total{decision}`, `rl_fallback_total{reason}`, `rl_redis_errors_total`,
`rl_breaker_open{shard}`, `rl_decision_latency_us_bucket{le}` (+ `_sum`, `_count`),
`rl_upstream_errors_total`.

## 9. Testing and benchmarks
- Unit (GoogleTest): limiters, hash ring, breaker, HTTP parser, event loop timers, config parser.
- Integration (GoogleTest + live Redis db 15): each Lua script. Small windows (200 ms) plus sleeps.
- Smoke: `bench/smoke.sh` brings the cluster up and curls an allowed, a denied and a metrics request.
- `bench/run_cluster.sh up|down <mode>`: Redis :6379/:6380, upstream :9000, gateways :8081-8083.
- `bench/wrk.sh`: upstream direct vs gateway in each mode, `wrk -t4 -c64 -d15s --latency`.
- `bench/accuracy.py`: 3 nodes, one key, limit L per window W, K windows; admitted vs L per window.
- `bench/chaos.py`: steady load; kill shard :6380 at t=5 s, restart at t=12 s; per-second
  allowed/denied/fallback from `/metrics`; time to breaker open and to recovery.
- Results recorded in `README.md` with machine details. wrk's coordinated-omission caveat noted.
