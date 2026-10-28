# Theory to Study

Organized by build day: study a topic the day you build it. Each topic has
**Know**: what you must be able to explain, **Here**: where it shows up in this code,
and **Q**: questions an interviewer will ask. Answer every Q out loud before you
call a day done. Items marked ★ are the highest-yield for interviews.

Claude adds to this file as the build goes. Anything that surprised us while
building goes under "Lessons from the build" at the bottom.

---

## Day 1: Rate-limiting algorithms ★ (you implement these)

### Why rate limit at all
**Know**
- Protect a service from overload (one noisy client shouldn't starve everyone else).
- Security: slow down brute-force logins, credential stuffing and scraping.
- Cost control (paid APIs, LLM calls) and fairness between tenants or plan tiers.
- Where it lives: client side (polite), at the edge/gateway (this project), or inside the service.

### Fixed window counter
**Know**
- `window = now / W`, one counter per (key, window). Allow while `count < limit`.
- O(1) time and memory. Trivial to do in Redis with `INCR` + `EXPIRE`.
- **Boundary burst:** with 100/min, a client can send 100 at 0:59 and 100 at 1:00, which is
  200 requests in about 1 second, double the intended rate.
- Windows are epoch-aligned, so every client's window resets at the same instant. That can cause
  synchronized bursts (a thundering herd at the top of every minute).

### Sliding window log
**Know**
- Store the timestamp of every accepted request. Drop timestamps older than `now - W`.
  Allow while the count is below `limit`.
- Exact: there's no boundary burst.
- Memory is O(limit) per key: 10M users × 100 timestamps × 8 bytes is about 8 GB. That's
  why it's rarely used for high limits.
- In Redis it's a ZSET with score = timestamp: `ZREMRANGEBYSCORE`, `ZCARD`, `ZADD`.

### Sliding window counter ★
**Know**
- Keep only the current and previous fixed-window counts. Estimate the rolling count as
  `prev × (W - elapsed)/W + curr`.
- This assumes requests in the previous window were spread evenly. It's approximate but O(1).
- Cloudflare's write-up on their rate limiter reports a very small error rate with this method
  across hundreds of millions of requests. It's the usual "best trade-off" answer.
- Use the integer form `prev×(W−elapsed) + curr×W < limit×W` to avoid floating-point surprises.

### Token bucket ★
**Know**
- A bucket of capacity `b` refills at rate `r` tokens/sec. Each request takes 1 token.
- It allows bursts up to `b`, while the long-run average is capped at `r`.
- **Lazy refill:** you don't need a timer per bucket. On each request, compute
  `tokens = min(b, tokens + elapsed × r)` and store `(tokens, last_ts)`. Only touched keys cost CPU.
- Used by AWS API Gateway and Stripe (Stripe's blog on scaling with rate limiters describes it).
- Two parameters (burst and rate) rather than one, so it can express "a burst of 20, then 5/sec".

### Leaky bucket (theory only)
**Know**
- *As a queue:* requests enter a FIFO that drains at a constant rate. It smooths output, which
  is traffic **shaping**, but queued requests add latency.
- *As a meter:* mathematically the same as a token bucket, just inverted.
- **GCRA** (generic cell rate algorithm) is a leaky-bucket variant that stores a single number per
  key, the "theoretical arrival time". The `redis-cell` module uses it. Worth one sentence in an
  interview.

### Comparison table (memorize)
| Algorithm | Memory/key | Accuracy | Bursts | Typical use |
|---|---|---|---|---|
| Fixed window | 1 counter | Up to 2× at boundary | Yes, at boundary | Simple quotas |
| Sliding log | O(limit) | Exact | No | Low limits, strict |
| Sliding counter | 2 counters | Approximate, very close | Smoothed | General API limits |
| Token bucket | 2 numbers | Exact for its model | Yes, up to `b` | APIs that allow bursts |
| Leaky bucket (queue) | Queue | Exact | No (shaped) | Traffic shaping |

**Here:** `src/limiters.cpp`, and the same algorithms in `lua/*.lua`.

**Q**
- Walk me through the fixed-window boundary problem with numbers.
- Why does token bucket not need a background refill thread?
- Sliding log is exact. Why not always use it?
- What does `remaining` mean for each algorithm? What should `Retry-After` be?
- How would you support "100/min AND 1000/hour" at once? (Check every rule; deny if any denies.
  Only consume from all of them if all allow, otherwise a denied request still eats quota.)

---

## Day 1: I/O models and epoll ★ (Claude builds this; you must explain it)

### Blocking vs non-blocking I/O
**Know**
- A blocking `read()` puts the thread to sleep until data arrives. Thread-per-connection
  means 10k clients need 10k threads, each with about 8 MB of virtual stack and context-switch cost:
  the **C10K problem**.
- Non-blocking (`O_NONBLOCK`): `read()` returns -1 with `errno == EAGAIN/EWOULDBLOCK` instead of
  waiting. You then need a way to learn *when* an fd is ready, which is the job of multiplexing.

### select / poll / epoll
**Know**
- `select`: a bitmap of fds, capped at `FD_SETSIZE` = 1024, O(n) scan, and the set is rebuilt each call.
- `poll`: an array, no fd cap, still O(n) per call.
- `epoll`: the kernel keeps the interest list (`epoll_ctl` ADD/MOD/DEL) and `epoll_wait` returns
  **only the ready fds**, so cost is O(ready), not O(watched). macOS/BSD use `kqueue` and Windows uses IOCP.
- `io_uring` (newer Linux): a completion-based model with submission/completion ring buffers that
  avoids a syscall per operation. Know that it exists and why: fewer syscalls, true async file I/O.

### Level-triggered vs edge-triggered ★
**Know**
- **Level-triggered** (default): `epoll_wait` keeps reporting the fd while data remains unread.
  Forgiving.
- **Edge-triggered** (`EPOLLET`): reported once per *change*. You **must** read until `EAGAIN`,
  or the leftover bytes never trigger another event and the connection hangs.
- This project uses level-triggered for simplicity. Write interest (`EPOLLOUT`) is turned on only
  while there is pending output; otherwise the loop spins, because a socket is almost always writable.

### The reactor pattern
**Know**
- One loop: wait for events, then dispatch each to its handler. Handlers must **never block**.
  One blocking call (a sync Redis call, a DNS lookup, a disk read) stalls every connection on that loop.
- Timers: keep a min-heap of deadlines and pass `epoll_wait` a timeout equal to the time until the
  nearest one. (Alternative: `timerfd`.)
- Used by nginx, Redis, Node.js (libuv) and Netty.
- Scaling out: one loop per core. Either N processes sharing a port with `SO_REUSEPORT`, or N
  threads each with their own loop.

**Here:** `src/event_loop.cpp`, and the hiredis adapter in `src/redis_async.cpp`.

**Q**
- Why is epoll faster than select for 10k mostly idle connections?
- What goes wrong with edge-triggered epoll if you read only once?
- Why does a single blocking call inside an event loop hurt so much?
- How does your gateway use multiple cores without locks?
- What's the difference between the reactor (epoll) and proactor (IOCP/io_uring) models?

---

## Day 2: TCP sockets and HTTP/1.1 (Claude builds; you explain)

### Sockets
**Know**
- Server sequence: `socket → setsockopt → bind → listen(backlog) → accept`. Client: `socket → connect`.
- The `listen` backlog bounds the queue of completed handshakes not yet `accept`ed
  (capped by `net.core.somaxconn`).
- `SO_REUSEADDR` lets you re-bind a port with connections still in `TIME_WAIT` (fast restarts).
  `SO_REUSEPORT` lets *several* sockets bind the same port; the kernel load-balances new
  connections between them by hashing the 4-tuple.
- `TIME_WAIT`: the side that closes first holds the 4-tuple for 2×MSL (60 s on Linux) so stray
  late packets aren't misread as part of a new connection. Opening a new connection per request
  exhausts ephemeral ports, which is why we **pool** upstream connections.
- `TCP_NODELAY` disables Nagle's algorithm. Nagle combined with delayed ACKs can add about 40 ms of
  latency to small request/response traffic.
- A non-blocking `connect` returns `EINPROGRESS`. The socket turns writable when the connect
  finishes; read `SO_ERROR` to learn whether it succeeded.
- `SIGPIPE`: writing to a socket the peer closed kills the process by default. Ignore it, or pass
  `MSG_NOSIGNAL`.

### HTTP/1.1 framing
**Know**
- Request line, then headers, a blank line (`\r\n\r\n`), then the body. Header names are
  **case-insensitive**.
- TCP is a byte stream. One `read()` can return half a request or two requests. The parser must
  buffer and resume (incremental parsing).
- The body length comes from `Content-Length` or `Transfer-Encoding: chunked` (not supported here).
- Connections are persistent by default in 1.1. `Connection: close` ends one.
- Pipelining exists but is rarely used because of head-of-line blocking (one slow response holds
  up every later one). HTTP/2 multiplexing fixes that.
- Rate-limit responses: `429 Too Many Requests` (RFC 6585) and `Retry-After` (seconds or an HTTP date).
  `X-RateLimit-Limit/Remaining/Reset` are a convention (GitHub uses them). There is also an IETF
  draft standardizing `RateLimit` / `RateLimit-Policy` headers.
- Request smuggling (one to know about): if a proxy and a backend disagree on where a request ends
  (for example, both `Content-Length` and `Transfer-Encoding` present), an attacker can sneak a
  request through. Our parser rejects ambiguous framing with 400.

### Reverse proxy and connection pooling
**Know**
- The gateway terminates the client connection and opens its own to the upstream. Pooling keeps
  idle keep-alive upstream connections for reuse, which saves a TCP handshake (1 RTT) and avoids
  `TIME_WAIT` build-up.
- Timeouts everywhere: connect, read and idle. Without them, one stuck upstream holds client
  connections open forever (slowloris-style resource exhaustion).

**Here:** `src/http.cpp`, `src/server.cpp`, `src/upstream_pool.cpp`.

**Q**
- What happens when `read()` returns only part of the headers?
- Why are upstream connections pooled? What is `TIME_WAIT`?
- `SO_REUSEADDR` vs `SO_REUSEPORT`?
- What is Nagle's algorithm, and when do you turn it off?
- Which status code and headers does a rate limiter return?

---

## Day 2: Redis internals and Lua atomicity ★ (you write the Lua)

**Know**
- Redis executes commands on **one thread**, one at a time. Redis 6+ "I/O threads" only parallelize
  socket reads/writes, not command execution. So every single command is atomic.
- A **Lua script runs atomically**: no other client's command runs in the middle of it. That's what
  turns "read, check, write" into one indivisible step without locks.
- The flip side: a slow script blocks *all* clients. Keep scripts O(1) or O(log n).
  (`busy-reply-threshold`, formerly `lua-time-limit`, defaults to 5 s, after which Redis answers BUSY.)
- `SCRIPT LOAD` returns a SHA1. `EVALSHA sha numkeys k... args...` then sends only the hash, which
  saves bandwidth. After a Redis restart or `SCRIPT FLUSH`, Redis answers `NOSCRIPT`. The client must
  reload the script and retry. The gateway handles this.
- `redis.call('TIME')` inside the script returns `{seconds, microseconds}` from the **Redis server
  clock**. Every gateway node then sees one clock, so there's no clock skew between nodes. This is
  allowed because Redis 7 replicates the script's *effects* (the writes), not the script itself.
- Expiry: keys get a TTL (`PEXPIRE`, in ms). Redis deletes expired keys lazily (when touched) and
  actively (it samples a handful of keys with TTLs about 10 times a second). Always set a TTL on
  rate-limit keys, or memory grows forever.
- Data structures used here: STRING counters (`INCR`, `INCRBY`), ZSET (a skiplist plus a hash table,
  O(log n) insert and range delete), HASH (token bucket `{tokens, ts}`).
- Redis numbers: Lua numbers are doubles. Redis converts a Lua number to an **integer** reply by
  truncating it. Return fractional values as strings, or round them deliberately.

### Redis Cluster caveat (good interview depth)
- Redis Cluster splits keys over 16384 **hash slots**: `CRC16(key) mod 16384`. A script may only
  touch keys in one slot, and it must list them in `KEYS`.
- Our scripts derive extra keys inside Lua (`key..":seq"`, `key..":"..window`). That's fine with our
  own client-side sharding, because derived keys land on the same instance as `KEYS[1]`. On Redis
  Cluster you'd use **hash tags**: `rl:{user42}:seq` hashes only the part inside `{}`.

**Here:** `lua/*.lua`, `src/redis_async.cpp`.

**Q**
- Why is a Lua script atomic in Redis? What's the cost of that guarantee?
- Why take time from Redis `TIME` instead of the gateway clock?
- What happens to `EVALSHA` after Redis restarts?
- How do you keep rate-limit keys from growing memory forever?
- How would these scripts change to run on Redis Cluster?

---

## Day 3: Race conditions in distributed systems ★

**Know**
- **Check-then-act** / read-modify-write: two gateways both `GET count = 99`, both decide "allowed",
  both `SET 100`. Two requests passed but the counter grew by one. That's the **lost update** problem.
  Under load across N nodes the over-admission is large, and our `race.py` measures it.
- Fixes, from weakest to strongest fit here:
  1. Atomic single commands: `INCR` returns the new value, so check *after* incrementing.
  2. Optimistic locking: `WATCH key` / `MULTI` / `EXEC`. The transaction aborts if the key changed,
     so the client retries. Under contention that means a lot of retries.
  3. **Lua script:** the whole decision runs atomically on the server, in one round trip. This is our choice.
  4. Distributed locks (Redlock etc.): overkill, slower, and their correctness under clock jumps and
     GC pauses is debated. Mention it only to reject it.
- "Atomic" vs "isolated" vs "linearizable": know that linearizable means every operation appears to
  take effect at a single instant between its call and its response.

**Here:** `naive` mode in `src/decider.cpp` vs your Lua scripts; `bench/race.py`.

**Q**
- Show me the race in GET-then-SET with two nodes.
- Why not use a distributed lock?
- Compare WATCH/MULTI with Lua scripts.

---

## Day 3: Designs for distributed rate limiting ★

**Know**: the main options and when each fits.
1. **Local only, limit/N per node:** no shared state and zero added latency. It's only correct if the
   load balancer spreads each client evenly over nodes, and it breaks when nodes are added or removed.
2. **Sticky routing:** route each client to one gateway (by consistent-hashing the client key at the
   load balancer), then limit locally. Exact and fast, but there's a hot-spot risk and a client's
   counts reset when its node changes.
3. **Centralized store (Redis), one call per request:** exact and simple. Costs one network round trip
   (about 0.1-1 ms in a data centre) per request, and Redis becomes a dependency. Our `redis` mode.
4. **Hybrid leasing/batching:** each node *leases* a chunk of quota from Redis and spends it locally.
   One Redis call per chunk instead of per request. Error is bounded by the unspent leases,
   about `nodes × chunk` per window. Our `hybrid` mode. Google's Doorman quota system uses
   similar ideas.
5. **Gossip / CRDT counters:** nodes keep local counts and exchange them periodically
   (G-counter = per-node counts, summed). No central store and eventually consistent; over-admits
   between syncs.
- Scale estimate to have ready: 10M keys × about 100 bytes is about 1 GB, which fits in one Redis.
  A single Redis handles roughly 100k+ simple ops/sec; beyond that, shard by key. That's why
  consistent hashing comes next.

**Q**
- Design a rate limiter for 1M req/s across 50 gateways. (Hybrid leasing plus sharded Redis.
  Explain the error bound.)
- What if one user is extremely hot? (Their key lands on one shard. Use local leasing for that key.
  You can't split a single key's counter cheaply without losing accuracy.)

---

## Day 3: Consistent hashing ★ (you implement it)

**Know**
- Naive sharding with `hash(key) % N` is bad when N changes to N+1: about N/(N+1) of keys move,
  so almost everything is remapped and every counter resets at once.
- **Ring:** hash nodes and keys onto the same 64-bit circle. A key belongs to the first node
  clockwise from it. Adding a node moves only the keys between it and its predecessor, about
  1/(N+1) of them. (Karger et al., 1997.)
- **Virtual nodes:** place each physical node at about 100-200 points (`hash(node + "#" + i)`).
  This evens out the arcs (with only 1 point per node, arc sizes vary wildly) and lets you weight
  bigger machines with more vnodes.
- Lookup: keep the ring as a sorted `std::map<uint64_t, std::string>`. Use
  `lower_bound(hash(key))` and wrap to `begin()` at the end. That's O(log V).
- Hash quality matters. FNV-1a is simple but mixes short, similar strings (`"a#1"`, `"a#2"`) poorly.
  Run a finalizer such as **fmix64** (from MurmurHash3) or **splitmix64** on the result so every
  input bit affects every output bit (the *avalanche* property).
- Alternatives, one line each: **Rendezvous (HRW) hashing** gives each key to the node with the
  highest `hash(key, node)`; it needs no ring but lookup is O(N). **Jump consistent hash**
  (Lamping & Veach) is O(1) memory and very even, but you can only add or remove at the end.
  **Maglev hashing** (Google) uses a lookup table for fast, even load balancing.
  **Redis Cluster** uses fixed hash slots (16384) instead of a ring; rebalancing moves whole slots.

**Here:** `src/hash_ring.cpp`, used to pick the Redis shard in `src/decider.cpp`.

**Q**
- Why not `hash % N`? Quantify the remap.
- What are virtual nodes for?
- What happens to rate-limit counters on the keys that move? (They start fresh on the new shard:
  briefly under-counted. That's acceptable for rate limits, but not for a database.)
- Ring vs Redis Cluster hash slots?

---

## Day 4: Failure handling, timeouts and circuit breakers ★ (you implement the breaker)

**Know**
- **Timeouts** are mandatory on every remote call. Our Redis budget is 50 ms; past that, the decision
  is worth less than the latency it costs.
- **Circuit breaker** (from Michael Nygard's *Release It!*):
  - Closed: calls pass and failures are counted.
  - Open: calls fail fast without touching the dependency, which gives it room to recover and
    stops threads/requests from piling up behind timeouts.
  - Half-open: after a cool-down, let one probe through. Success closes the breaker; failure
    re-opens it.
- Why fail fast: without a breaker, every request waits the full 50 ms timeout while Redis is down,
  and latency explodes.
- **Retries:** only for idempotent operations, with exponential backoff **and jitter** (random
  spread). Otherwise every client retries at the same instant (a retry storm or thundering herd).
  We don't retry rate-limit calls; we fall back instead.
- **Bulkhead:** isolate resources per dependency (one breaker per Redis shard here) so one bad shard
  doesn't take down decisions for the others.

### Fail-open vs fail-closed ★
- **Fail-open:** when the limiter's state is unavailable, allow traffic. Availability wins, at the
  risk of overload. Suits general API traffic.
- **Fail-closed:** deny. Safety wins. Suits `/login` (stops brute force during an outage) and
  expensive endpoints.
- **Degrade to local:** use a local limiter with `limit / N`. A middle ground, and our `local` fallback.
- **CAP:** when the gateway is partitioned from Redis, it chooses Availability (open/local) or
  Consistency (closed). **PACELC** adds: even with no partition, there's a Latency vs Consistency
  trade-off. Hybrid leasing gives up some consistency to save latency.

**Q**
- Draw the breaker state machine. What triggers each transition?
- Why one breaker per shard instead of one global breaker?
- Which endpoints fail open, and which fail closed? Why?
- Why add jitter to backoff?

---

## Day 4: Clocks and time

**Know**
- **Wall clock** (`system_clock`, `CLOCK_REALTIME`) can jump backwards (NTP, manual changes).
  **Monotonic clock** (`steady_clock`, `CLOCK_MONOTONIC`) only moves forward. Use it for durations
  and timeouts.
- Across machines, clocks drift by milliseconds even with NTP. If each gateway computed window
  boundaries from its own clock, nodes would disagree about which window a request belongs to.
  Taking time from Redis `TIME` avoids that.
- Passing `now_ms` into `Limiter::allow` (instead of reading the clock inside) is **dependency
  injection**. It's what makes the tests deterministic.

---

## Day 5: Observability and benchmarking ★

**Know**
- Counters (things that only go up) vs gauges (current values) vs **histograms** (bucketed
  distributions, which let you compute percentiles).
- **Percentiles beat averages:** the mean hides the tail. p99 = 1 in 100 requests is slower than
  this. A page that makes 50 backend calls hits a backend's p99 on most loads.
- **Little's law:** `L = λ × W` (requests in flight = arrival rate × latency). Use it to sanity-check
  wrk results and connection counts.
- **Coordinated omission** (Gil Tene, "How NOT to Measure Latency"): closed-loop load generators like
  `wrk` wait for each response before sending the next request. When the server stalls, wrk sends
  less load and *under-reports* tail latency. `wrk2` fixes this with a constant rate (`-R`).
  Mention this caveat when quoting p99 from wrk.
- Method: warm up, measure a **baseline** (upstream directly), change one variable at a time, run
  each test several times, and report the machine and settings.

**Q**
- Why report p99 and not the average?
- What is coordinated omission?
- What did your gateway add to latency, and where does that time go?

---

## C++ topics that come up in this codebase

- **RAII** for file descriptors: a wrapper that `close()`s in its destructor, so no fd leaks on early returns.
- **Callback lifetime ★:** a Redis reply can arrive *after* the client connection closed. A callback
  holding a raw `Connection*` is then a use-after-free. We store a connection **id** and look it up
  when the reply arrives (alternative: `weak_ptr`).
- `std::function` for callbacks, plus lambdas and their capture semantics (`[this]` is a raw pointer).
- `std::string_view`: cheap to pass, but never store one past the buffer it points into. Buffers get
  reallocated when they grow.
- `std::map::lower_bound` for the ring; `std::unordered_map` for per-key limiter state, plus
  what you'd do about unbounded growth (evict idle keys).
- `std::priority_queue` (a min-heap via `std::greater`) for timers.

---

## The "Design a rate limiter" interview answer (outline to practise)

1. **Requirements:** per-user/IP/API-key limits, multiple rules, low added latency (< 1-2 ms),
   distributed, highly available, clear client feedback (429 + headers).
2. **Placement:** API gateway / middleware, in front of the services.
3. **Algorithm:** token bucket or sliding window counter, with the trade-off table.
4. **Storage:** Redis with an atomic Lua script, a TTL on every key, server-side time.
5. **Distributed:** shard counters by key with consistent hashing. For extreme scale, hybrid
   leasing, with its error bound.
6. **Failures:** timeouts, a circuit breaker per shard, fail-open vs fail-closed per rule.
7. **Observability:** allowed/denied counters, decision latency p99, fallback rate, Redis errors.
8. **Numbers:** quote your own benchmark results from the README.

## Resources (in priority order)
1. Alex Xu, *System Design Interview* Vol. 1, chapter "Design a Rate Limiter".
2. Cloudflare blog: "How we built rate limiting capable of scaling to millions of domains"
   (sliding window counter).
3. Stripe blog: "Scaling your API with rate limiters" (token bucket, fail-open choices).
4. `man 7 epoll`, plus *Beej's Guide to Network Programming* (sockets, non-blocking I/O).
5. Redis docs: "Scripting with Lua" and "EVAL" (atomicity, `TIME`, effects replication).
6. Karger et al. 1997, "Consistent Hashing and Random Trees" (skim the intro), and the
   Jump Consistent Hash paper (2 pages).
7. Michael Nygard, *Release It!*, the chapter on stability patterns (circuit breaker, bulkhead, timeouts).
8. Gil Tene, "How NOT to Measure Latency" (talk) on coordinated omission.

---

## Lessons from the build
(Filled in as we go: bugs we hit and what they taught us.)

**Day 1**
- **Stale events in an epoll batch ★.** `epoll_wait` hands back up to 128 ready events at once. If the
  callback for event #1 closes the fd behind event #2, a naive loop still runs #2's handler: a
  use-after-free, or worse, a call into a *new* connection that reused the same fd number. The fix in
  `event_loop.cpp`: `epoll_event.data` holds a unique registration id instead of the fd, and dispatch
  skips ids that are gone. A callback that deletes *itself* is kept alive by copying its `shared_ptr`
  before the call. Good answer to "what's hard about writing an event loop?"
- **Level-triggered `EPOLLOUT` spins.** An idle socket is always writable, so asking for `EPOLLOUT`
  permanently makes the loop busy-wait at 100% CPU. Ask only while there's unsent output (test
  `ModFdSwitchesInterest`).
- **Tests must fail cleanly.** The first version of the Lua tests indexed into an empty reply when a
  script returned an error, so against the stubs they *segfaulted* instead of failing. A red test
  should tell you *what's* wrong. The helper now returns sentinel values and reports the Redis error text.
- **Exactness at boundaries.** The exact sliding-counter `retry_after` can be `window_ms + 1`: at the
  first ms of the next window the previous window still weighs 100%. The first draft of the test had
  `≤ window_ms` and would have rejected a correct solution. Validating tests against a reference
  implementation caught it.
- **HTTP framing is a security boundary.** The parser rejects `Transfer-Encoding`, duplicate
  conflicting `Content-Length`, spaces in header names (obsolete line folding) and double spaces in
  the request line. Being strict is the defense against request smuggling.
