# Distributed Rate Limiter

An HTTP gateway in C++17 that enforces one rate limit across several nodes. Each node is a
single-threaded epoll reactor; the shared count lives in Redis, updated by atomic Lua scripts
and sharded with consistent hashing. When a Redis shard dies, per-shard circuit breakers switch
traffic to a fallback within a quarter of a second.

Measured on one machine (details below):

- **Race:** with a non-atomic GET-then-SET, 3 nodes let **600 of 600** requests through a limit of 100.
  With the Lua script, exactly **100**.
- **Accuracy:** one user at 3x the limit across 3 nodes. Per-node limiting admits **+201%**;
  Redis mode **+0%**; hybrid leasing **+1%**.
- **Failure:** a Redis shard killed under 500 req/s. Breakers opened on all nodes in **222 ms**
  and closed **3.2 s** after it came back, with **0 failed requests**.
- **Throughput:** one gateway thread proxies **20k req/s** with a Redis script on every request.
  Hybrid leasing gets that to **27k**, close to the 29k of purely local limiting, while keeping the
  shared limit.

## How it works

```
client ──HTTP──► gateway node (x3, :8081-8083)
                  │ epoll loop (one thread per process)
                  │ ├─ HTTP/1.1 parser → rule match → client key (IP or a header)
                  │ ├─ decision: local | redis | hybrid | naive
                  │ │     redis key → consistent hash ring → shard (:6379, :6380)
                  │ │                 └─ circuit breaker per shard → fail mode
                  │ ├─ allowed → proxy to upstream over pooled keep-alive connections
                  │ └─ denied  → 429 + Retry-After + X-RateLimit-* headers
                  └─ GET /metrics (Prometheus), GET /healthz
upstream: the same binary with --serve-upstream
```

Rules come from a file:

```
# name  method path_prefix key_by            algo             limit  window_ms  fail
login   POST   /login      ip                token_bucket     5      60000      closed
api     *      /api        header:X-User-Id  sliding_counter  100    60000      local
```

**Algorithms** (`src/limiters.cpp`, and the same logic in `lua/*.lua`): fixed window, sliding log,
sliding window counter and token bucket. Each has a header comment explaining the idea and its
weakness. Start reading there.

**Decision modes** (`--mode`):

| mode | how | shared across nodes | Redis calls |
|---|---|---|---|
| `local` | in-process limiter | no: N nodes admit N x the limit | none |
| `redis` | the rule's Lua script, atomic inside Redis | yes | one per request |
| `hybrid` | each node leases a chunk of the window's quota and spends it locally | yes | one per chunk |
| `naive` | `GET`, compare, `SET` in two round trips | racy, on purpose | two per request |

**Sharding:** keys are spread over Redis shards by a consistent hash ring with 100 virtual nodes
per shard (`src/hash_ring.cpp`). Adding a fourth shard moves about a quarter of the keys, all of them
to the new shard; `hash % N` would move three quarters.

**Failures:** every Redis call has a 50 ms timeout. Five failures in a row open that shard's circuit
breaker for 2 s (`src/circuit_breaker.cpp`); then one probe request decides whether it closes. While
Redis can't answer, each rule's `fail` column decides: `open` lets requests through, `closed`
returns 429, `local` uses an in-process limiter with `limit / nodes`.

**Time** comes from Redis (`TIME` inside the scripts), so nodes with skewed clocks still agree on
window edges.

## Build and run

Linux (tested on WSL2 Ubuntu 24.04) with g++ 13, CMake, Ninja, hiredis, GoogleTest, Redis 7 and wrk:

```bash
sudo apt install g++ cmake ninja-build pkg-config libhiredis-dev libgtest-dev redis-server wrk
cmake -S . -B build -G Ninja && cmake --build build
(cd build && ctest)          # 76 tests; the Lua ones need Redis on :6379
```

Run the 3-node cluster and try it (`/deny` allows 1 request per minute per IP):

```bash
export BUILD_DIR=$PWD/build
bench/run_cluster.sh up redis
for port in 8081 8082 8083; do curl -s -o /dev/null -w '%{http_code}\n' localhost:$port/deny; done
curl -s localhost:8081/metrics
bench/run_cluster.sh down
```

The first request gets 200 and the other two get 429, even though each went to a different node.

Benchmarks (each starts and stops its own cluster):

```bash
python3 bench/race.py       # naive vs redis vs hybrid, one window
python3 bench/accuracy.py   # local vs redis vs hybrid, 5 windows
python3 bench/chaos.py      # kill a shard under load
bench/wrk.sh                # throughput and latency
MODE=hybrid bench/smoke.sh  # end-to-end checks against one gateway, any mode
```

## Results

Machine: Intel Core i5-12450H laptop (8 cores, 12 threads), WSL2 Ubuntu 24.04 VM with 7 GB of RAM,
Redis 7.0.15. Everything (wrk, the Python clients, 3 gateways, the upstream and 2 Redis
instances) runs on the same machine, so they compete for the same cores.

### Throughput and latency (`bench/wrk.sh`)

`wrk -t4 -c64 -d15s` against one gateway node (one thread), 10,000 distinct users, median of 3 runs.
The rule's limit is 1e9, so nothing is denied unless the row says so.

| target | req/s | p50 | p99 |
|---|---|---|---|
| upstream directly (no gateway) | 85,969 | 0.68 ms | 2.77 ms |
| gateway, local mode | 28,795 | 2.10 ms | 13.16 ms |
| gateway, redis mode (a Lua script per request) | 19,985 | 3.02 ms | 6.96 ms |
| gateway, hybrid mode (leased quota) | 27,375 | 2.15 ms | 7.86 ms |
| gateway, redis mode, every request denied | 31,392 | 1.83 ms | 6.65 ms |
| gateway, hybrid mode, every request denied | 56,785 | 0.89 ms | 6.95 ms |

- The proxy hop is the main cost. Every allowed request means two sockets and two HTTP parses on
  one thread, which is why the gateway is about 3x slower than the upstream alone. The "every request
  denied" rows skip the upstream and show the decision path on its own.
- A Redis call per request costs about 30% of throughput (local 28.8k vs redis 20.0k). Hybrid gets
  most of it back, because each user needs one lease, not one call per request.
- Hybrid denials are the fastest path: once Redis says a window's quota is gone, a node denies
  locally until the window ends.
- Tail latency is noisy on a shared laptop: hybrid's three p99s were 11.3, 7.9 and 7.2 ms. wrk also
  measures latency from when each request is actually sent, so it under-reports the tail when the
  server stalls (coordinated omission). Treat p99 as a lower bound.

### Race: non-atomic vs atomic (`bench/race.py`)

60 clients, 600 attempts, 3 nodes, one user, limit 100 per 10 s fixed window.

| mode | admitted | over the limit |
|---|---|---|
| naive (GET, then SET) | 600 | +500% |
| redis (Lua) | 100 | +0% |
| hybrid (leases) | 100 | +0% |

With dozens of requests in flight, they all read the same count, all decide "allowed" and all
write count + 1. The counter barely moves. An earlier run admitted 558; the exact number depends
on timing, which is the problem.

### Accuracy across nodes (`bench/accuracy.py`)

One user sends 3x their limit (200 per 2 s, sliding window counter) for 5 windows, round-robin over
3 nodes. "Max in any 2 s" is the strictest check: the most admissions in any 2 s span.

| mode | admitted per 2 s window | max in any 2 s | over the limit |
|---|---|---|---|
| local | 600, 599, 600, 599, 600 | 602 | +201% |
| redis | 200, 200, 200, 200, 200 | 201 | +0% |
| hybrid | 200, 200, 200, 200, 200 | 202 | +1% |

Hybrid leases are fixed windows, so a sliding span across a window edge can catch a little more.
It never exceeds the limit within a window, because Redis never hands out more than the limit and
leased tokens expire with their window.

### A shard dies under load (`bench/chaos.py`)

Redis mode, 2 shards, 40 users, 500 req/s in total. Shard :6380 shut down at t=5 s, restarted at t=12 s.

| | |
|---|---|
| breakers open on all 3 nodes | 222 ms after the kill |
| breakers closed again | 3.2 s after the restart (Redis start + 500 ms reconnect + 2 s open period) |
| failed requests (not 200 or 429) | 0 of 9,000 |

Users on the dead shard fell back to each node's local limiter. Metrics are scraped every 250 ms,
so the times are accurate to about that.

## Design decisions

- **One thread per process, no locks.** The nginx and Redis model: an epoll reactor handles every
  socket, and scaling means more processes (`SO_REUSEPORT`). Redis calls are asynchronous; a request
  is a continuation that resumes when the reply or timeout arrives.
- **Our own hiredis adapter** plugs hiredis into the event loop with six hooks, so Redis I/O and
  timeouts run on the same loop as HTTP.
- **Ids, not pointers, across callbacks.** A reply can arrive after its client disconnected, so
  callbacks look the client up by id.
- **Strict HTTP parsing.** `Transfer-Encoding` and conflicting `Content-Length` are rejected, which
  rules out request smuggling at the cost of not supporting chunked bodies.
- **Fail mode is per rule.** A login rule should fail closed so an outage can't become a brute-force
  window; a general API rule can fail open or to a local limit.

`docs/THEORY.md` covers the theory behind each of these, with interview questions and the bugs we
hit along the way.

## Limits of this version

- Per-key state is never evicted, so memory grows with distinct keys. It needs an idle-key sweep
  before keying on client IPs in production.
- Scripts derive extra keys (`key:window`, `key:seq`), which Redis Cluster would reject without
  hash tags. Here each shard is an independent Redis.
- No TLS, chunked encoding, HTTP/2 or config reload. Rules are read once at startup.
- In hybrid mode, `X-RateLimit-Remaining` is what this node has leased, not the global count.
- All numbers come from one machine; a real deployment adds network round trips to every Redis call.
