# Distributed Rate Limiter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement
> the Claude-owned tasks task by task. Steps use checkbox (`- [ ]`) syntax for tracking.
> Tasks marked **[ALI]** are written by Ali by hand. Claude never writes their implementation;
> it only reviews, and gives hints when asked.

**Goal:** A C++17 epoll HTTP gateway that enforces per-key rate limits consistently across
3 nodes using sharded Redis, with measured accuracy, latency and failure behaviour.

**Architecture:** One single-threaded epoll reactor per process. Redis is called through
hiredis's async API on our own loop. Allowed requests are proxied to an upstream over
pooled keep-alive connections. Four decision modes (local / redis / hybrid / naive)
let the benchmarks compare the trade-offs.

**Tech Stack:** C++17, g++ 13.3, CMake 3.28 + Ninja, hiredis 1.2, Redis 7.0.15,
GoogleTest 1.14, wrk 4.1, Python 3.12 (stdlib only). WSL2 Ubuntu 24.04.

**Spec:** `docs/specs/2026-10-26-rate-limiter-design.md`. **Theory:** `docs/THEORY.md`.

## Global Constraints

- Linux only (WSL distro `Ubuntu-24.04`, not the default `Ubuntu`). Build and run everything inside it.
- C++17, `-Wall -Wextra -Werror` on our sources. No new dependencies beyond the list above.
- No blocking calls on the event loop thread (no sync Redis, no `sleep`, no blocking sockets).
- Every Redis rate-limit key gets a `PEXPIRE`. Time inside Lua comes only from `redis.call('TIME')`.
- Redis key format `rl:<rule>:<client_key>`. Lua return `{allowed, remaining, retry_after_ms}`, all integers.
- `Limiter::allow` takes `now_ms` from the caller; no clock reads inside limiters.
- Files are LF line endings (`.gitattributes`), otherwise bash and Lua break.
- Python scripts: stdlib only.

## How to build and test (inside WSL)

```bash
wsl -d Ubuntu-24.04
cd <repo path under /mnt/c/...>
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build
ctest --test-dir build --output-on-failure          # everything
ctest --test-dir build --output-on-failure -R FixedWindow   # one group
```

## A note on code in this plan

The **contracts** (headers) and **tests** for Ali's units are written out in full,
because they are the interface between us. Claude's plumbing tasks list the exact
interface, behaviour and test cases. Claude writes that code while executing, since
pasting about 2k lines of C++ here as well would just be a second copy to keep in sync.

## File map

```
CMakeLists.txt                 build: lib `rlcore` + `gateway` exe + one test exe per area
.gitattributes .gitignore
src/types.h                    Decision, Algo                                   (Claude)
src/limiters.h / .cpp          Limiter + 4 algorithms          header Claude / .cpp ALI
src/hash_ring.h / .cpp         consistent hashing             header Claude / .cpp ALI
src/circuit_breaker.h / .cpp   breaker state machine          header Claude / .cpp ALI
lua/*.lua                      5 Redis scripts                                  ALI
src/event_loop.h / .cpp        epoll + timer heap                               (Claude)
src/http.h / .cpp              request/response parsing + serializing           (Claude)
src/config.h / .cpp            rules file, CLI flags, make_limiter()            (Claude)
src/metrics.h / .cpp           counters, latency histogram, Prometheus text     (Claude)
src/upstream_pool.h / .cpp     pooled non-blocking upstream connections         (Claude)
src/redis_async.h / .cpp       hiredis epoll adapter, EVALSHA, timeouts         (Claude)
src/decider.h / .cpp           local/redis/hybrid/naive + fallback + breakers   (Claude)
src/server.h / .cpp            accept, connections, rules, 429, proxy           (Claude)
src/main.cpp                   wiring                                           (Claude)
tests/test_*.cpp               GoogleTest
bench/run_cluster.sh smoke.sh wrk.sh accuracy.py race.py chaos.py rules.bench.conf
docs/THEORY.md                 study notes (Claude keeps adding)
README.md                      results + how to run
```

## Day-by-day schedule

| Day | Claude | Ali (about 4-6 h coding + 1-2 h theory) |
|---|---|---|
| 1 | C1 scaffold, C2 contracts + red tests, C3 event loop, C4 HTTP | Theory Day 1 → **U1 limiters** |
| 2 | C5 config + metrics, C6 server + proxy + local mode | Theory Day 2 → **U2 Lua scripts** |
| 3 | C7 async Redis, C8 redis + naive modes, cluster script, race demo | Theory Day 3 → **U3 hash ring** |
| 4 | C9 sharding + breakers + fallback + hybrid, C10 benchmarks | Theory Day 4 → **U4 circuit breaker** |
| 5 | C11 run benchmarks, README, resume bullets | **U5** explain-back: walkthrough + Q drill |

Dependencies: C6 needs U1 (local mode uses your limiters). C8 needs U2. C9 needs U3 and U4.
If you're running late, Claude builds against the stubs and the feature lights up when your tests go green.

---

## Task C1: Scaffold (Claude, Day 1)

**Files:** create `CMakeLists.txt`, `.gitattributes`, `.gitignore`, `src/types.h`, `tests/test_smoke.cpp`.

**Interfaces, produces:**
```cpp
// src/types.h
#pragma once
#include <cstdint>
struct Decision {
  bool allowed;
  int64_t remaining;       // >= 0
  int64_t retry_after_ms;  // 0 when allowed
};
enum class Algo { FixedWindow, SlidingLog, SlidingCounter, TokenBucket };
```
CMake targets: `rlcore` (static lib, all `src/*.cpp` except `main.cpp`), `gateway` (exe), and test
executables `test_limiters test_hash_ring test_breaker test_lua test_event_loop test_http test_config
test_redis_async`, each linked to `rlcore GTest::gtest_main`, registered with `gtest_discover_tests`.
`LUA_DIR` compile definition = `${CMAKE_SOURCE_DIR}/lua`. hiredis is found with
`find_package(hiredis)`, falling back to `pkg_check_modules(hiredis)`.

- [ ] Step 1: write the files. `.gitattributes`: `* text=auto eol=lf`. `.gitignore`: `build/`.
- [ ] Step 2: `tests/test_smoke.cpp` with `TEST(Smoke, Builds) { SUCCEED(); }`. Configure, build, `ctest`. Expected: 1 test passes.
- [ ] Step 3: commit `chore: scaffold CMake, GoogleTest, shared types`.

## Task C2: Contracts and red tests for Ali's units (Claude, Day 1)

**Files:** create `src/limiters.h`, `src/limiters.cpp` (stubs), `src/hash_ring.h`, `src/hash_ring.cpp` (stubs),
`src/circuit_breaker.h`, `src/circuit_breaker.cpp` (stubs), `lua/{fixed_window,sliding_log,sliding_counter,token_bucket,lease}.lua` (stubs),
`tests/test_limiters.cpp`, `tests/test_hash_ring.cpp`, `tests/test_breaker.cpp`, `tests/test_lua.cpp`.

Stubs compile and return "wrong" values (`{false,0,0}`, throw, `redis.error_reply("TODO(Ali)")`),
so every test starts **red**.

- [ ] Step 1: write the headers and stubs exactly as in U1-U4 below.
- [ ] Step 2: write the test files exactly as in U1-U4 below.
- [ ] Step 3: **validate the tests.** Write throwaway reference implementations in
  `/tmp/ref/` (outside the repo), build the tests against them, and confirm all green. Recalibrate any
  statistical bound that's flaky, then delete `/tmp/ref`. Never commit the reference code.
- [ ] Step 4: build against the stubs and confirm the tests fail (red) and nothing else breaks.
- [ ] Step 5: commit `test: contracts and failing tests for limiters, ring, breaker, lua`.

---

## Task U1 [ALI]: Local limiters (Day 1, about 5-7 h)

Study first: THEORY.md → "Day 1: Rate-limiting algorithms".

**Files:** modify only `src/limiters.cpp` (and the private members in `src/limiters.h`).
**Done when:** `ctest --test-dir build -R "FixedWindow|SlidingLog|SlidingCounter|TokenBucket"` is all green.

**Contract (given):**
```cpp
// src/limiters.h
#pragma once
#include <cstdint>
#include <deque>
#include <string>
#include <unordered_map>
#include "types.h"

class Limiter {
 public:
  virtual ~Limiter() = default;
  // now_ms: caller-supplied monotonic milliseconds (fake clock in tests).
  virtual Decision allow(const std::string& key, int64_t now_ms) = 0;
};

class FixedWindowLimiter : public Limiter {
 public:
  FixedWindowLimiter(int64_t limit, int64_t window_ms);
  Decision allow(const std::string& key, int64_t now_ms) override;
 private:
  int64_t limit_, window_ms_;
  // TODO(Ali): per-key state
};

class SlidingLogLimiter : public Limiter { /* same shape */ };
class SlidingCounterLimiter : public Limiter { /* same shape */ };
class TokenBucketLimiter : public Limiter { /* same shape; capacity = limit, rate = limit / window_ms per ms */ };
```

**Semantics:** see spec §5.2. The tests below are the source of truth.

**Tests (given, `tests/test_limiters.cpp`):**
```cpp
#include <gtest/gtest.h>
#include "limiters.h"

// ---------- Fixed window ----------
TEST(FixedWindow, AllowsUpToLimitThenDenies) {
  FixedWindowLimiter l(3, 1000);
  for (int i = 0; i < 3; i++) {
    Decision d = l.allow("u", 100);
    EXPECT_TRUE(d.allowed) << "request " << i;
    EXPECT_EQ(d.remaining, 2 - i);
    EXPECT_EQ(d.retry_after_ms, 0);
  }
  Decision d = l.allow("u", 100);
  EXPECT_FALSE(d.allowed);
  EXPECT_EQ(d.remaining, 0);
  EXPECT_EQ(d.retry_after_ms, 900);  // window [0,1000) ends at 1000
}
TEST(FixedWindow, ResetsAtWindowBoundary) {
  FixedWindowLimiter l(2, 1000);
  l.allow("u", 998);
  l.allow("u", 999);
  EXPECT_FALSE(l.allow("u", 999).allowed);
  EXPECT_TRUE(l.allow("u", 1000).allowed);
}
TEST(FixedWindow, BoundaryBurstIsTheKnownWeakness) {
  FixedWindowLimiter l(5, 1000);
  int ok = 0;
  for (int i = 0; i < 5; i++) ok += l.allow("u", 999).allowed;
  for (int i = 0; i < 5; i++) ok += l.allow("u", 1000).allowed;
  EXPECT_EQ(ok, 10);  // 2x the limit in 1 ms: this is why sliding windows exist
}
TEST(FixedWindow, KeysAreIndependent) {
  FixedWindowLimiter l(1, 1000);
  EXPECT_TRUE(l.allow("a", 0).allowed);
  EXPECT_TRUE(l.allow("b", 0).allowed);
  EXPECT_FALSE(l.allow("a", 0).allowed);
}

// ---------- Sliding log ----------
TEST(SlidingLog, NoBoundaryBurst) {
  SlidingLogLimiter l(5, 1000);
  int ok = 0;
  for (int i = 0; i < 5; i++) ok += l.allow("u", 999).allowed;
  for (int i = 0; i < 5; i++) ok += l.allow("u", 1000).allowed;
  EXPECT_EQ(ok, 5);
}
TEST(SlidingLog, OldEntriesExpireExactly) {
  SlidingLogLimiter l(2, 1000);
  EXPECT_TRUE(l.allow("u", 0).allowed);
  EXPECT_TRUE(l.allow("u", 500).allowed);
  Decision d = l.allow("u", 999);
  EXPECT_FALSE(d.allowed);
  EXPECT_EQ(d.retry_after_ms, 1);           // entry at t=0 leaves at t=1000
  EXPECT_TRUE(l.allow("u", 1000).allowed);  // window is (now-1000, now]
  EXPECT_FALSE(l.allow("u", 1000).allowed);
}
TEST(SlidingLog, RemainingCountsDown) {
  SlidingLogLimiter l(3, 1000);
  EXPECT_EQ(l.allow("u", 0).remaining, 2);
  EXPECT_EQ(l.allow("u", 1).remaining, 1);
  EXPECT_EQ(l.allow("u", 2).remaining, 0);
  EXPECT_EQ(l.allow("u", 3).remaining, 0);
}

// ---------- Sliding window counter ----------
TEST(SlidingCounter, ActsLikeFixedWindowInFirstWindow) {
  SlidingCounterLimiter l(10, 1000);
  for (int i = 0; i < 10; i++) EXPECT_TRUE(l.allow("u", 100).allowed);
  EXPECT_FALSE(l.allow("u", 100).allowed);
}
TEST(SlidingCounter, WeightsPreviousWindow) {
  SlidingCounterLimiter l(10, 1000);
  for (int i = 0; i < 10; i++) l.allow("u", i);  // prev window full
  // t=1500: prev weight 0.5 -> estimate 5 + curr; 5 more fit
  int ok = 0;
  for (int i = 0; i < 10; i++) ok += l.allow("u", 1500).allowed;
  EXPECT_EQ(ok, 5);
}
TEST(SlidingCounter, NoBoundaryBurst) {
  SlidingCounterLimiter l(10, 1000);
  for (int i = 0; i < 10; i++) l.allow("u", 999);
  EXPECT_FALSE(l.allow("u", 1000).allowed);  // prev weight ~1.0
}
TEST(SlidingCounter, SkippedWindowsForgetOldCounts) {
  SlidingCounterLimiter l(10, 1000);
  for (int i = 0; i < 10; i++) l.allow("u", 0);
  int ok = 0;  // window 2: previous window (1) was empty
  for (int i = 0; i < 10; i++) ok += l.allow("u", 2500).allowed;
  EXPECT_EQ(ok, 10);
}
TEST(SlidingCounter, DeniedHasSaneRetryAfterAndFirstRemaining) {
  SlidingCounterLimiter l(10, 1000);
  EXPECT_EQ(l.allow("u", 0).remaining, 9);
  for (int i = 0; i < 9; i++) l.allow("u", 0);
  Decision d = l.allow("u", 0);
  EXPECT_FALSE(d.allowed);
  EXPECT_GT(d.retry_after_ms, 0);
  EXPECT_LE(d.retry_after_ms, 1001);  // exact answer here is 1001: at t=1000 prev still weighs 1.0
}

// ---------- Token bucket (capacity 10, refill 10 per 1000 ms) ----------
TEST(TokenBucket, StartsFullThenDenies) {
  TokenBucketLimiter l(10, 1000);
  EXPECT_EQ(l.allow("u", 0).remaining, 9);
  for (int i = 0; i < 9; i++) EXPECT_TRUE(l.allow("u", 0).allowed);
  Decision d = l.allow("u", 0);
  EXPECT_FALSE(d.allowed);
  EXPECT_NEAR(d.retry_after_ms, 100, 1);  // one token every 100 ms
}
TEST(TokenBucket, RefillsLazily) {
  TokenBucketLimiter l(10, 1000);
  for (int i = 0; i < 10; i++) l.allow("u", 0);
  EXPECT_FALSE(l.allow("u", 50).allowed);   // 0.5 token
  EXPECT_TRUE(l.allow("u", 100).allowed);   // 1 token
  EXPECT_FALSE(l.allow("u", 100).allowed);
}
TEST(TokenBucket, NeverExceedsCapacity) {
  TokenBucketLimiter l(10, 1000);
  l.allow("u", 0);
  int ok = 0;  // idle for a long time, burst is still capped at 10
  for (int i = 0; i < 20; i++) ok += l.allow("u", 1000000).allowed;
  EXPECT_EQ(ok, 10);
}
```

**Hints (no solutions):**
- Fixed window: what identifies "the current window" for a key? What do you store per key so you
  know when to reset?
- Sliding log: which end of a `std::deque<int64_t>` holds the oldest entry? Prune *before* checking.
- Sliding counter: store `(window_index, curr, prev)`. When the request's window index moves on by
  exactly 1, `prev = curr`. When it moves on by more than 1, what should `prev` be?
- Token bucket: store `(tokens as double, last_ms)`. Compute `retry_after` as
  `ceil((1 - tokens) * window_ms / limit)` to avoid floating error from a precomputed rate.
- Commit when green: `feat: implement local rate-limit algorithms`.

---

## Task C3: Event loop (Claude, Day 1)

**Files:** `src/event_loop.h/.cpp`, `tests/test_event_loop.cpp`.

**Produces:**
```cpp
class EventLoop {
 public:
  using IoCb = std::function<void(uint32_t events)>;   // EPOLLIN/EPOLLOUT/EPOLLERR/EPOLLHUP bits
  using TimerCb = std::function<void()>;
  EventLoop();  ~EventLoop();                             // epoll_create1(EPOLL_CLOEXEC)
  void add_fd(int fd, uint32_t events, IoCb cb);          // level-triggered
  void mod_fd(int fd, uint32_t events);
  void del_fd(int fd);                                    // safe to call from inside a callback
  uint64_t add_timer(int64_t delay_ms, TimerCb cb);       // returns timer id
  void cancel_timer(uint64_t id);                         // lazy cancel (skipped when popped)
  void run();                                             // until stop()
  void stop();
  static int64_t now_ms();                                // steady_clock
};
```
Timer heap: `std::priority_queue` min-heap of `(deadline, id)`, with `epoll_wait` timeout = time to the earliest deadline.

**Tests:** timers fire in deadline order (30, 10, 20 → 10, 20, 30); a cancelled timer never fires;
a pipe write triggers the read callback with `EPOLLIN`; `del_fd` inside its own callback doesn't crash
and stops further callbacks; `stop()` from a timer makes `run()` return.

- [ ] Write failing tests → implement → green → commit `feat: epoll event loop with timer heap`.

## Task C4: HTTP/1.1 parsing (Claude, Day 1)

**Files:** `src/http.h/.cpp`, `tests/test_http.cpp`.

**Produces:**
```cpp
struct Header { std::string name, value; };
struct HttpRequest {
  std::string method, target, version;
  std::vector<Header> headers;
  std::string body;
  bool keep_alive = true;
  const std::string* header(std::string_view name) const;  // case-insensitive
};
struct HttpResponse {
  int status = 200;
  std::vector<Header> headers;
  std::string body;
  bool keep_alive = true;
};
enum class ParseStatus { Incomplete, Done, Error, TooLarge };
ParseStatus parse_request(std::string& buf, HttpRequest& out);    // consumes bytes on Done
ParseStatus parse_response(std::string& buf, HttpResponse& out);  // for upstream replies
std::string serialize(const HttpRequest&);   // for forwarding upstream
std::string serialize(const HttpResponse&);  // sets Content-Length, Connection
```
Limits: header block 16 KB → `TooLarge`. `Transfer-Encoding` present → `Error` (unsupported and
smuggling-safe). Both TE and CL → `Error`. HTTP/1.0 defaults to close; `Connection: close` overrides.

**Tests:** whole request in one buffer; the same request fed one byte at a time (`Incomplete` until
the last byte); two pipelined requests in one buffer parse one after the other; case-insensitive header
lookup; POST body with Content-Length; HTTP/1.0 → `keep_alive == false`; `Connection: close`;
`Transfer-Encoding: chunked` → Error; 17 KB of headers → TooLarge; garbage request line → Error;
a response with a body split across two buffers.

- [ ] Write failing tests → implement → green → commit `feat: incremental HTTP/1.1 parser and serializer`.

---

## Task C5: Config, CLI and metrics (Claude, Day 2)

**Files:** `src/config.h/.cpp`, `src/metrics.h/.cpp`, `tests/test_config.cpp`.

**Produces:**
```cpp
enum class Mode { Local, Redis, Hybrid, Naive };
enum class FailMode { Open, Closed, Local };
struct Rule {
  std::string name, method /* or "*" */, path_prefix, key_by /* "ip" or "header:X-Name" */;
  Algo algo; int64_t limit, window_ms; FailMode fail;
};
struct Options {
  int port = 8080; bool serve_upstream = false;
  std::string upstream_host = "127.0.0.1"; int upstream_port = 9000;
  std::string rules_path; Mode mode = Mode::Local;
  std::vector<std::pair<std::string,int>> redis;  // shards
  int nodes = 1; int64_t redis_timeout_ms = 50;
};
std::vector<Rule> parse_rules(std::istream& in);       // throws std::runtime_error("line N: ...")
Options parse_args(int argc, char** argv);             // throws on unknown flag
std::unique_ptr<Limiter> make_limiter(Algo, int64_t limit, int64_t window_ms);

class Metrics {
 public:
  void inc(const std::string& name_with_labels, int64_t by = 1);  // e.g. "rl_requests_total{decision=\"allowed\"}"
  void set(const std::string& name_with_labels, int64_t value);   // gauges
  void observe_latency_us(int64_t us);   // buckets 50,100,250,500,1000,2500,5000,10000,50000,+Inf
  std::string render() const;            // Prometheus text format
};
```
**Tests:** parse the sample rules from spec §6 (2 rules, all fields); comment and blank lines ignored;
unknown algo → exception mentioning "line 3"; `parse_args` for both example command lines in spec §6;
`render()` includes cumulative histogram buckets plus `_sum` and `_count`.

- [ ] Failing tests → implement → green → commit `feat: rules config, CLI options, metrics`.

## Task C6: Server, proxy, local mode (Claude, Day 2)

**Files:** `src/upstream_pool.h/.cpp`, `src/server.h/.cpp`, `src/decider.h/.cpp` (local mode only for now),
`src/main.cpp`, `bench/smoke.sh`, `bench/rules.bench.conf`.

**Produces:**
```cpp
using DecideCb = std::function<void(Decision, bool fallback_used)>;
class Decider {   // grows in C8/C9
 public:
  Decider(EventLoop&, const Options&, const std::vector<Rule>&, Metrics&);
  void decide(const Rule&, const std::string& client_key, DecideCb cb);  // may complete sync or async
};
class Server {
 public:
  Server(EventLoop&, const Options&, std::vector<Rule>, Metrics&, Decider*);  // Decider null in upstream mode
  void listen();   // SO_REUSEADDR + SO_REUSEPORT, non-blocking, TCP_NODELAY on accepted sockets
};
```
Behaviour (spec §6, §7): connections are tracked by **id** in an `unordered_map<uint64_t, Conn>`,
and async callbacks capture the id, not a pointer. One request at a time per client connection.
Idle timeout 10 s. `/healthz` → 200. `/metrics` → `metrics.render()`. Upstream mode: every request
→ `200 ok`. Gateway: rule match → decide → proxy (adding `X-RateLimit-Limit/Remaining`) or 429
(adding `Retry-After`). Upstream errors/timeouts (2 s) → 502. SIGPIPE ignored.

**Test (`bench/smoke.sh`, run manually and in C11):** start upstream :9000 and gateway :8081
(`--mode local`) with rule `smoke * /limited ip fixed_window 3 60000 closed`.
Expect: `/limited` ×3 → 200; 4th → 429 with `Retry-After`; `/free` → 200 (no rule);
`/metrics` contains `rl_requests_total{decision="denied"} 1`; stop upstream → `/free` → 502.

- [ ] Implement → `bench/smoke.sh` passes → commit `feat: gateway server, upstream pool, local mode`.

---

## Task U2 [ALI]: Lua scripts (Day 2, about 4-5 h)

Study first: THEORY.md → "Day 2: Redis internals and Lua atomicity".
**Files:** `lua/fixed_window.lua`, `lua/sliding_log.lua`, `lua/sliding_counter.lua`, `lua/token_bucket.lua`, `lua/lease.lua`.
**Done when:** `ctest --test-dir build -R Lua` is all green (Redis must be running: `redis-cli ping` → PONG).

**Contract:** `EVALSHA sha 1 KEYS[1] ARGV[1]=limit ARGV[2]=window_ms [ARGV[3]=chunk]`.
- Time: `local t = redis.call('TIME'); local now = t[1]*1000 + math.floor(t[2]/1000)`.
- Return `{allowed, remaining, retry_after_ms}` as **integers** (`math.floor`/`math.ceil` yourself;
  Redis truncates Lua floats).
- Every key you touch gets `PEXPIRE` so it disappears after it can no longer matter.
- `lease.lua` returns `{grant, ms_until_window_end}`.
- Derived keys (`KEYS[1]..":"..window`, `KEYS[1]..":seq"`) are fine here; see THEORY "Redis Cluster caveat".

**Tests (given, `tests/test_lua.cpp`; uses sync hiredis on db 15, skipped if Redis is down):**
```cpp
#include <gtest/gtest.h>
#include <hiredis/hiredis.h>
#include <atomic>
#include <chrono>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>

static std::string load(const std::string& name) {
  std::ifstream f(std::string(LUA_DIR) + "/" + name);
  std::stringstream ss; ss << f.rdbuf(); return ss.str();
}
static void sleep_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

class Lua : public ::testing::Test {
 protected:
  redisContext* c = nullptr;
  void SetUp() override {
    c = redisConnect("127.0.0.1", 6379);
    if (!c || c->err) GTEST_SKIP() << "Redis not running on :6379";
    freeReplyObject(redisCommand(c, "SELECT 15"));
    freeReplyObject(redisCommand(c, "FLUSHDB"));
  }
  void TearDown() override { if (c) redisFree(c); }
  // Runs a script, returns its integer array. Fails the test on a Redis error.
  std::vector<long long> run(const std::string& file, const std::string& key,
                             long long limit, long long window, long long chunk = -1) {
    std::string src = load(file);
    redisReply* r = chunk < 0
      ? (redisReply*)redisCommand(c, "EVAL %s 1 %s %lld %lld", src.c_str(), key.c_str(), limit, window)
      : (redisReply*)redisCommand(c, "EVAL %s 1 %s %lld %lld %lld", src.c_str(), key.c_str(), limit, window, chunk);
    std::vector<long long> out;
    EXPECT_NE(r, nullptr);
    if (!r) return out;
    EXPECT_EQ(r->type, REDIS_REPLY_ARRAY) << (r->type == REDIS_REPLY_ERROR ? r->str : "not an array");
    if (r->type == REDIS_REPLY_ARRAY)
      for (size_t i = 0; i < r->elements; i++) {
        EXPECT_EQ(r->element[i]->type, REDIS_REPLY_INTEGER) << "element " << i << " must be an integer";
        out.push_back(r->element[i]->integer);
      }
    freeReplyObject(r);
    return out;
  }
  void expect_all_keys_expire() {
    redisReply* keys = (redisReply*)redisCommand(c, "KEYS *");
    ASSERT_GT(keys->elements, 0u);
    for (size_t i = 0; i < keys->elements; i++) {
      redisReply* t = (redisReply*)redisCommand(c, "PTTL %s", keys->element[i]->str);
      EXPECT_GT(t->integer, 0) << keys->element[i]->str << " has no TTL";
      freeReplyObject(t);
    }
    freeReplyObject(keys);
  }
};

TEST_F(Lua, FixedWindowLimitsAndExpires) {
  for (int i = 0; i < 3; i++) {
    auto r = run("fixed_window.lua", "rl:t:u", 3, 300);
    ASSERT_EQ(r.size(), 3u);
    EXPECT_EQ(r[0], 1); EXPECT_EQ(r[1], 2 - i);
  }
  auto d = run("fixed_window.lua", "rl:t:u", 3, 300);
  EXPECT_EQ(d[0], 0); EXPECT_GT(d[2], 0); EXPECT_LE(d[2], 300);
  expect_all_keys_expire();
  sleep_ms(d[2] + 20);
  EXPECT_EQ(run("fixed_window.lua", "rl:t:u", 3, 300)[0], 1);
}
TEST_F(Lua, SlidingLogCountsSameMillisecondRequests) {
  for (int i = 0; i < 3; i++) EXPECT_EQ(run("sliding_log.lua", "rl:t:u", 3, 300)[0], 1);
  auto d = run("sliding_log.lua", "rl:t:u", 3, 300);
  EXPECT_EQ(d[0], 0); EXPECT_GT(d[2], 0); EXPECT_LE(d[2], 300);
  expect_all_keys_expire();
  sleep_ms(320);
  EXPECT_EQ(run("sliding_log.lua", "rl:t:u", 3, 300)[0], 1);
}
TEST_F(Lua, SlidingCounterLimitsAndRecovers) {
  for (int i = 0; i < 4; i++) EXPECT_EQ(run("sliding_counter.lua", "rl:t:u", 4, 400)[0], 1);
  auto d = run("sliding_counter.lua", "rl:t:u", 4, 400);
  EXPECT_EQ(d[0], 0); EXPECT_GT(d[2], 0); EXPECT_LE(d[2], 401);
  expect_all_keys_expire();
  sleep_ms(850);  // two full windows later the old count has no weight
  EXPECT_EQ(run("sliding_counter.lua", "rl:t:u", 4, 400)[0], 1);
}
TEST_F(Lua, TokenBucketRefills) {
  for (int i = 0; i < 4; i++) EXPECT_EQ(run("token_bucket.lua", "rl:t:u", 4, 400)[0], 1);
  auto d = run("token_bucket.lua", "rl:t:u", 4, 400);  // 1 token per 100 ms
  EXPECT_EQ(d[0], 0); EXPECT_GT(d[2], 0); EXPECT_LE(d[2], 101);
  expect_all_keys_expire();
  sleep_ms(120);
  EXPECT_EQ(run("token_bucket.lua", "rl:t:u", 4, 400)[0], 1);
  EXPECT_EQ(run("token_bucket.lua", "rl:t:u", 4, 400)[0], 0);
}
TEST_F(Lua, LeaseGrantsChunksUntilQuotaIsGone) {
  std::vector<long long> grants;
  for (int i = 0; i < 4; i++) {
    auto r = run("lease.lua", "rl:t:u", 10, 1000, 4);
    ASSERT_EQ(r.size(), 2u);
    grants.push_back(r[0]);
    EXPECT_GT(r[1], 0); EXPECT_LE(r[1], 1000);
  }
  EXPECT_EQ(grants, (std::vector<long long>{4, 4, 2, 0}));
  expect_all_keys_expire();
}
TEST_F(Lua, FixedWindowIsAtomicUnderConcurrency) {
  std::string src = load("fixed_window.lua");
  std::atomic<int> allowed{0};
  std::vector<std::thread> ts;
  for (int t = 0; t < 8; t++) ts.emplace_back([&] {
    redisContext* cc = redisConnect("127.0.0.1", 6379);
    freeReplyObject(redisCommand(cc, "SELECT 15"));
    for (int i = 0; i < 50; i++) {
      redisReply* r = (redisReply*)redisCommand(cc, "EVAL %s 1 rl:t:hot 100 5000", src.c_str());
      if (r && r->type == REDIS_REPLY_ARRAY && r->element[0]->integer == 1) allowed++;
      freeReplyObject(r);
    }
    redisFree(cc);
  });
  for (auto& t : ts) t.join();
  EXPECT_EQ(allowed.load(), 100);  // 400 attempts, exactly the limit gets through
}
```
(Note: windows are epoch-aligned, so the lease and concurrency tests could straddle a window
boundary and see the quota reset. In C2, Claude adds a `wait_for_fresh_window(window_ms)` helper
that reads Redis `TIME` and sleeps until the current window has more than half its length left.
Both tests call it first.)

**Hints:**
- fixed_window: `INCR` returns the new count, so check after incrementing. Set `PEXPIRE` only when the count is 1.
- sliding_log: the members of a ZSET must be unique. Two requests in the same ms would collide, so
  `INCR` a `:seq` key for a unique suffix.
- sliding_counter: two keys, `KEYS[1]..":"..w` and `..":"..(w-1)`. Which TTL keeps the previous window alive long enough?
- token_bucket: `HMGET key tokens ts`. Missing means a full bucket. Refill, take, `HSET`, `PEXPIRE`.
- lease: like fixed_window, but `INCRBY grant` where `grant = min(chunk, limit - used)`.
- Commit when green: `feat: atomic Lua rate-limit scripts`.

---

## Task C7: Async Redis client (Claude, Day 3)

**Files:** `src/redis_async.h/.cpp`, `tests/test_redis_async.cpp`.

**Produces:**
```cpp
struct RedisResult { bool ok; std::string error; std::vector<long long> ints; };
using RedisCb = std::function<void(RedisResult)>;
class RedisShard {
 public:
  RedisShard(EventLoop&, std::string host, int port, int64_t timeout_ms);
  void connect();                                         // async; reconnects every 500 ms while down
  bool connected() const;
  // Loads (SCRIPT LOAD) on connect; on NOSCRIPT reloads and retries once.
  void eval(const std::string& script_name, const std::string& key,
            std::vector<long long> args, RedisCb cb);
  void command(std::vector<std::string> argv, RedisCb cb);  // for naive GET/SET
  static void register_script(const std::string& name, std::string source);  // from LUA_DIR at startup
};
```
Our own hiredis adapter: hook `addRead/delRead/addWrite/delWrite/cleanup/scheduleTimer` into
`EventLoop` (see hiredis `adapters/` for the shape). Use `redisAsyncSetTimeout`.

**Tests (live Redis):** registered script `return {1,2,3}` → callback gets ints `{1,2,3}`; after
`SCRIPT FLUSH` the next `eval` still succeeds (NOSCRIPT reload path); with `CLIENT PAUSE 300 ALL`
issued from a second connection, an `eval` with a 50 ms timeout calls back with `ok=false` within 150 ms;
connecting to a closed port calls back `ok=false` quickly.

- [ ] Failing tests → implement → green → commit `feat: async hiredis client on our epoll loop`.

## Task C8: Redis and naive modes, cluster script, race demo (Claude, Day 3)

**Files:** modify `src/decider.cpp`; create `bench/run_cluster.sh`, `bench/race.py`.

- `redis` mode: one `RedisShard` (the first `--redis`) for now. `eval("<algo>", "rl:<rule>:<key>", {limit, window_ms})`.
  On error: fallback per rule (open/closed/local), counted as `rl_fallback_total{reason="redis_error"}`.
- `naive` mode: `GET` → compare with limit → `SET count+1 PX window_ms`. Two round trips, deliberately racy.
- `bench/run_cluster.sh up <mode> [rules]`: starts redis :6380 (`--save "" --appendonly no --daemonize yes`),
  upstream :9000, gateways :8081-8083 (logs to `/tmp/rl/*.log`, pids in `/tmp/rl/*.pid`).
  `down` kills them. :6379 is the system Redis.
- `bench/race.py`: 3 nodes, 60 concurrent asyncio clients on one key, rule limit 100 per 10 s,
  fixed window, 1 window. Prints admitted vs 100 for `naive` and then `redis`.
  Expected: naive admits far more than 100; redis admits exactly 100 (within a window boundary).

- [ ] Implement → run `race.py` → paste the numbers into the README draft → commit `feat: redis and naive decision modes, race demo`.

---

## Task U3 [ALI]: Consistent hash ring (Day 3, about 3 h)

Study first: THEORY.md → "Day 3: Consistent hashing".
**Files:** `src/hash_ring.cpp` (+ private members in the header). **Done when:** `ctest -R "HashRing|Hash64"` is green.

**Contract (given):**
```cpp
// src/hash_ring.h
#pragma once
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <string_view>

uint64_t hash64(std::string_view s);  // FNV-1a 64-bit followed by a fmix64 finalizer

class HashRing {
 public:
  explicit HashRing(int vnodes_per_node = 100);
  void add_node(const std::string& node);     // no-op if present
  void remove_node(const std::string& node);  // no-op if absent
  const std::string& node_for(std::string_view key) const;  // throws std::runtime_error if empty
  size_t size() const;                        // physical nodes
 private:
  int vnodes_;
  // TODO(Ali): your ring + node set
};
```
**Tests (given, `tests/test_hash_ring.cpp`):**
```cpp
#include <gtest/gtest.h>
#include <bitset>
#include <map>
#include "hash_ring.h"

static std::string K(int i) { return "rl:api:user" + std::to_string(i); }

TEST(Hash64, IsDeterministicAndAvalanches) {
  EXPECT_EQ(hash64("abc"), hash64("abc"));
  double bits = 0;
  for (int i = 0; i < 1000; i++)
    bits += std::bitset<64>(hash64("node#" + std::to_string(i)) ^
                            hash64("node#" + std::to_string(i + 1))).count();
  bits /= 1000;
  EXPECT_GT(bits, 28.0);  // good mixing flips about 32 of 64 bits
  EXPECT_LT(bits, 36.0);
}
TEST(HashRing, EmptyRingThrows) {
  HashRing r;
  EXPECT_THROW(r.node_for("x"), std::runtime_error);
}
TEST(HashRing, SingleNodeOwnsEverything) {
  HashRing r; r.add_node("a");
  for (int i = 0; i < 1000; i++) EXPECT_EQ(r.node_for(K(i)), "a");
}
TEST(HashRing, SizeCountsPhysicalNodes) {
  HashRing r; r.add_node("a"); r.add_node("b"); r.add_node("a");
  EXPECT_EQ(r.size(), 2u);
  r.remove_node("b"); r.remove_node("zzz");
  EXPECT_EQ(r.size(), 1u);
}
TEST(HashRing, InsertionOrderDoesNotMatter) {
  HashRing x, y;
  for (auto n : {"a", "b", "c"}) x.add_node(n);
  for (auto n : {"c", "a", "b"}) y.add_node(n);
  for (int i = 0; i < 5000; i++) EXPECT_EQ(x.node_for(K(i)), y.node_for(K(i)));
}
TEST(HashRing, DistributionIsReasonablyEven) {
  HashRing r; for (auto n : {"a", "b", "c"}) r.add_node(n);
  std::map<std::string, int> count;
  for (int i = 0; i < 30000; i++) count[r.node_for(K(i))]++;
  for (auto& [node, n] : count) {
    EXPECT_GT(n, 6000) << node;   // > 20%
    EXPECT_LT(n, 14100) << node;  // < 47%
  }
}
TEST(HashRing, AddingANodeMovesAboutAQuarterAllToTheNewNode) {
  HashRing r; for (auto n : {"a", "b", "c"}) r.add_node(n);
  std::vector<std::string> before;
  for (int i = 0; i < 30000; i++) before.push_back(r.node_for(K(i)));
  r.add_node("d");
  int moved = 0;
  for (int i = 0; i < 30000; i++) {
    const std::string& now = r.node_for(K(i));
    if (now != before[i]) { moved++; EXPECT_EQ(now, "d"); }
  }
  EXPECT_GT(moved, 4500);  // > 15%
  EXPECT_LT(moved, 10500); // < 35%   (ideal 25%)
}
TEST(HashRing, RemovingANodeMovesOnlyItsKeys) {
  HashRing r; for (auto n : {"a", "b", "c", "d"}) r.add_node(n);
  std::vector<std::string> before;
  for (int i = 0; i < 30000; i++) before.push_back(r.node_for(K(i)));
  r.remove_node("b");
  for (int i = 0; i < 30000; i++)
    if (before[i] != "b") EXPECT_EQ(r.node_for(K(i)), before[i]);
}
```
**Hints:** `std::map<uint64_t, std::string>` + `lower_bound`, wrapping to `begin()`.
Vnode label `node + "#" + std::to_string(i)`. FNV-1a 64: offset `14695981039346656037`, prime
`1099511628211`. fmix64: `h ^= h >> 33; h *= 0xff51afd7ed558ccd; h ^= h >> 33; h *= 0xc4ceb9fe1a85ec53; h ^= h >> 33;`.
Removing: erase exactly the vnode hashes you inserted for that node.
Commit when green: `feat: consistent hash ring with virtual nodes`.

---

## Task U4 [ALI]: Circuit breaker (Day 4, about 2-3 h)

Study first: THEORY.md → "Day 4: Failure handling".
**Files:** `src/circuit_breaker.cpp` (+ private members). **Done when:** `ctest -R Breaker` is green.

**Contract (given):**
```cpp
// src/circuit_breaker.h
#pragma once
#include <cstdint>
enum class BreakerState { Closed, Open, HalfOpen };
class CircuitBreaker {
 public:
  CircuitBreaker(int failure_threshold, int64_t open_ms);
  bool allow_request(int64_t now_ms);  // see spec §5.5
  void on_success();
  void on_failure(int64_t now_ms);
  BreakerState state() const;
 private:
  // TODO(Ali)
};
```
**Tests (given, `tests/test_breaker.cpp`):**
```cpp
#include <gtest/gtest.h>
#include "circuit_breaker.h"

TEST(Breaker, StartsClosedAndAllows) {
  CircuitBreaker b(3, 1000);
  EXPECT_EQ(b.state(), BreakerState::Closed);
  EXPECT_TRUE(b.allow_request(0));
}
TEST(Breaker, OpensAfterThresholdConsecutiveFailures) {
  CircuitBreaker b(3, 1000);
  b.on_failure(10); b.on_failure(20);
  EXPECT_EQ(b.state(), BreakerState::Closed);
  b.on_failure(30);
  EXPECT_EQ(b.state(), BreakerState::Open);
  EXPECT_FALSE(b.allow_request(31));
}
TEST(Breaker, SuccessResetsTheFailureCount) {
  CircuitBreaker b(3, 1000);
  b.on_failure(1); b.on_failure(2); b.on_success(); b.on_failure(3); b.on_failure(4);
  EXPECT_EQ(b.state(), BreakerState::Closed);
}
TEST(Breaker, StaysOpenUntilCooldownThenHalfOpens) {
  CircuitBreaker b(1, 1000);
  b.on_failure(100);
  EXPECT_FALSE(b.allow_request(1099));
  EXPECT_TRUE(b.allow_request(1100));
  EXPECT_EQ(b.state(), BreakerState::HalfOpen);
}
TEST(Breaker, HalfOpenAllowsExactlyOneProbe) {
  CircuitBreaker b(1, 1000);
  b.on_failure(0);
  EXPECT_TRUE(b.allow_request(1000));
  EXPECT_FALSE(b.allow_request(1001));
  EXPECT_FALSE(b.allow_request(5000));  // still waiting on the probe
}
TEST(Breaker, ProbeSuccessCloses) {
  CircuitBreaker b(1, 1000);
  b.on_failure(0); b.allow_request(1000); b.on_success();
  EXPECT_EQ(b.state(), BreakerState::Closed);
  EXPECT_TRUE(b.allow_request(1001));
}
TEST(Breaker, ProbeFailureReopensWithFreshTimer) {
  CircuitBreaker b(1, 1000);
  b.on_failure(0); b.allow_request(1000); b.on_failure(1000);
  EXPECT_EQ(b.state(), BreakerState::Open);
  EXPECT_FALSE(b.allow_request(1999));
  EXPECT_TRUE(b.allow_request(2000));
}
TEST(Breaker, LateRepliesWhileOpenAreIgnored) {
  CircuitBreaker b(1, 1000);
  b.on_failure(0);
  b.on_failure(900);  // a reply from a request sent before opening
  b.on_success();     // same
  EXPECT_EQ(b.state(), BreakerState::Open);
  EXPECT_TRUE(b.allow_request(1000));  // timer was not reset by the late failure
}
```
**Hints:** three fields are enough: state, consecutive failures and opened-at time (plus a
"probe in flight" flag, or let the HalfOpen state imply it). Write the transition table on paper first.
Commit when green: `feat: circuit breaker`.

---

## Task C9: Sharding, breakers, fallback, hybrid mode (Claude, Day 4)

**Files:** modify `src/decider.h/.cpp`, `src/main.cpp`.

- One `RedisShard` + one `CircuitBreaker(5, 2000)` per `--redis` entry. A `HashRing` holds the
  shard names `host:port`. The Redis key goes to `ring.node_for(key)`.
- Flow per spec §6 step 4. Breaker open → fallback with `reason="breaker_open"`; error/timeout
  → `on_failure`, fallback with `reason="redis_error"`. Gauge `rl_breaker_open{shard}`.
- Fallback `local`: a lazily created local limiter per rule with `limit / nodes` (min 1).
- `hybrid`: per (rule, key) `{tokens, expires_at_ms, pending queue, in_flight}`. Spend a token
  locally if one is left and unexpired; otherwise queue the request and, if no lease is in flight,
  call `lease.lua` with `chunk = max(1, limit / (nodes * 4))`. On reply: tokens = grant, and expiry =
  now + ms_until_window_end. Drain the queue (deny with retry = ms_until_window_end once tokens run out).
- `rl:<rule>:<key>` for all modes. Lease keys use the same name (only one mode runs at a time).

**Tests:** `bench/smoke.sh` extended: `--mode redis` with two shards → allowed/denied as in local
mode; `redis-cli -p 6380 shutdown nosave` → requests still answered (fallback counted in `/metrics`),
and after a restart, `rl_breaker_open` returns to 0 within about 3 s. Hybrid: `accuracy.py` (C10)
shows admitted ≤ limit + nodes×chunk.

- [ ] Implement → smoke passes in all 4 modes → commit `feat: sharded redis with breakers, fallback, hybrid leasing`.

## Task C10: Benchmarks (Claude, Day 4)

**Files:** `bench/wrk.sh`, `bench/accuracy.py`, `bench/chaos.py`.

- `wrk.sh`: for each target (upstream :9000 directly; gateway :8081 in local/redis/hybrid) run
  `wrk -t4 -c64 -d15s --latency -H "X-User-Id: $RANDOM-per-connection"` against a rule with a very
  high limit (we measure overhead, not denials). Output a markdown table: req/s, p50, p99.
  One extra run where every request is denied (429 path).
- `accuracy.py`: for modes local/redis/hybrid, 3 nodes, one key, limit 200 per 2000 ms window,
  sliding_counter (hybrid uses the lease). Send at about 3× the limit for 5 windows, round-robin over
  nodes. Print admitted per window, mean, max, and over-admission % vs the limit.
- `chaos.py`: redis mode, 2 shards, keys spread over both, steady 500 req/s. At t=5 s
  `redis-cli -p 6380 shutdown nosave`; at t=12 s restart it. Every 250 ms scrape `/metrics` on all nodes.
  Print a timeline (allowed, denied, fallback, breaker state) plus time-to-open and time-to-recover.

- [ ] Write scripts → dry-run each once → commit `bench: wrk, accuracy, chaos scripts`.

---

## Task C11: Results, README, resume bullets (Claude, Day 5)

- [ ] `bench/run_cluster.sh` + every benchmark. Run wrk 3× and keep the median.
- [ ] `README.md`: what it is, an architecture diagram, how to run, a results table (with machine:
  12 cores, WSL2, plus the wrk coordinated-omission caveat), design decisions and trade-offs
  (link THEORY.md), and the limits of this version.
- [ ] Resume bullets drafted from the **measured** numbers only (with the `human-writing` skill).
- [ ] Append "Lessons from the build" to THEORY.md.
- [ ] Commit `docs: results and README`.

## Task U5 [ALI]: Explain-back (Day 5, about 2 h)

- [ ] Claude walks you through `event_loop.cpp`, `http.cpp`, `server.cpp`, `redis_async.cpp` and
  `decider.cpp`. You explain each one back in your own words.
- [ ] Claude asks every **Q** in THEORY.md, mock-interview style, and marks the weak ones to revisit.
- [ ] You give the 8-step "Design a rate limiter" answer out loud in under 10 minutes, using your own numbers.

---

## Self-review (done while writing)
- Spec coverage: §5.2 → U1; §5.3 → U2; §5.4 → U3; §5.5 → U4; §6 → C5, C6, C8, C9; §7 → C6, C7, C9;
  §8 → C5; §9 → C10, C11. No gaps.
- Names match across tasks: `Decision`, `Algo`, `Limiter::allow`, `HashRing::node_for`,
  `CircuitBreaker::allow_request/on_success/on_failure`, `RedisShard::eval`, `Decider::decide`.
- Known risk: the Lua concurrency test can straddle a window boundary; C2 validation handles it.
