#include "decider.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <optional>

namespace {

constexpr int64_t kFailClosedRetryMs = 1000;
constexpr int64_t kErrorLogIntervalMs = 1000;
constexpr int kBreakerFailureThreshold = 5;
constexpr int64_t kBreakerOpenMs = 2000;
// Each node leases 1/4 of its fair share at a time: small enough that quota
// stranded on an idle node stays small, big enough to save most round trips.
constexpr int64_t kLeasesPerNodeShare = 4;

const std::vector<std::string> kScriptNames = {"fixed_window", "sliding_log", "sliding_counter", "token_bucket",
                                               "lease"};

LuaScripts scripts_for(const Options& options) {
  bool uses_lua = options.mode == Mode::Redis || options.mode == Mode::Hybrid;
  return uses_lua ? load_lua_scripts(options.lua_dir, kScriptNames) : LuaScripts{};
}

std::string redis_key(const Rule& rule, const std::string& client_key) {
  return "rl:" + rule.name + ":" + client_key;
}

std::string breaker_gauge(const std::string& shard_name) {
  return "rl_breaker_open{shard=\"" + shard_name + "\"}";
}

int64_t lease_chunk(const Rule& rule, int nodes) {
  return std::max<int64_t>(1, rule.limit / (nodes * kLeasesPerNodeShare));
}

// Wall-clock time, as each gateway sees it. Only naive mode uses it: the
// atomic modes take time from Redis so that every node agrees on one clock.
int64_t wall_clock_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// Lua scripts reply {allowed (0|1), remaining, retry_after_ms}.
std::optional<Decision> decision_from_script(const RedisResult& result) {
  constexpr size_t kFields = 3;
  if (!result.ok || result.values.size() != kFields) return std::nullopt;
  Decision decision;
  decision.allowed = result.values[0] == 1;
  decision.remaining = std::max<int64_t>(0, result.values[1]);
  decision.retry_after_ms = std::max<int64_t>(0, result.values[2]);
  return decision;
}

std::string describe_failure(const RedisResult& result, size_t expected_values) {
  if (!result.ok) return result.error;
  return "script returned " + std::to_string(result.values.size()) + " values, expected " +
         std::to_string(expected_values);
}

}  // namespace

Decider::Decider(EventLoop& loop, const Options& options, Metrics& metrics)
    : options_(options), metrics_(metrics), scripts_(scripts_for(options)) {
  if (options_.mode == Mode::Local) return;
  for (const auto& [host, port] : options_.redis) {
    auto redis = std::make_unique<RedisShard>(loop, host, port, options_.redis_timeout_ms, scripts_);
    const std::string name = redis->name();
    auto [shard, added] =
        shards_.try_emplace(name, Shard{std::move(redis), CircuitBreaker(kBreakerFailureThreshold, kBreakerOpenMs)});
    if (!added) continue;  // the same shard listed twice
    ring_.add_node(name);
    update_breaker_gauge(shard->second);
    shard->second.redis->connect();
  }
}

void Decider::decide(const Rule& rule, const std::string& client_key, DecideCb on_decided) {
  switch (options_.mode) {
    case Mode::Local:
      on_decided(local_limiter(rule).allow(client_key, EventLoop::now_ms()));
      return;
    case Mode::Redis:
      decide_redis(rule, client_key, std::move(on_decided));
      return;
    case Mode::Naive:
      decide_naive(rule, client_key, std::move(on_decided));
      return;
    case Mode::Hybrid:
      decide_hybrid(rule, client_key, std::move(on_decided));
      return;
  }
}

void Decider::decide_redis(const Rule& rule, const std::string& client_key, DecideCb on_decided) {
  const std::string key = redis_key(rule, client_key);
  Shard* shard = available_shard(key);
  if (!shard) {
    fall_back(rule, client_key, "breaker_open", on_decided);
    return;
  }
  const Rule* rule_ptr = &rule;  // rules live in the Server for the whole run
  shard->redis->eval(algo_name(rule.algo), key, {rule.limit, rule.window_ms},
                     [this, shard, rule_ptr, client_key, on_decided = std::move(on_decided)](RedisResult result) {
                       record_outcome(*shard, result.ok);
                       std::optional<Decision> decision = decision_from_script(result);
                       if (!decision) {
                         note_redis_error(describe_failure(result, 3));
                         fall_back(*rule_ptr, client_key, "redis_error", on_decided);
                         return;
                       }
                       on_decided(*decision);
                     });
}

// Read, decide, then write in a second round trip. Another gateway can read the
// same count in between, and both admit: the lost-update race that Lua prevents.
void Decider::decide_naive(const Rule& rule, const std::string& client_key, DecideCb on_decided) {
  const int64_t now = wall_clock_ms();
  const int64_t window = now / rule.window_ms;
  const int64_t retry_after_ms = (window + 1) * rule.window_ms - now;
  const std::string base_key = redis_key(rule, client_key);
  const std::string key = base_key + ":" + std::to_string(window);
  Shard* shard = available_shard(base_key);
  if (!shard) {
    fall_back(rule, client_key, "breaker_open", on_decided);
    return;
  }
  const Rule* rule_ptr = &rule;

  shard->redis->command({"GET", key}, [this, shard, rule_ptr, client_key, key, retry_after_ms,
                                       on_decided = std::move(on_decided)](RedisResult got) {
    record_outcome(*shard, got.ok);
    if (!got.ok) {
      note_redis_error(got.error);
      fall_back(*rule_ptr, client_key, "redis_error", on_decided);
      return;
    }
    const int64_t count = got.values.empty() ? 0 : got.values.front();
    if (count >= rule_ptr->limit) {
      on_decided(Decision{false, 0, retry_after_ms});
      return;
    }
    std::vector<std::string> set = {"SET", key, std::to_string(count + 1), "PX", std::to_string(rule_ptr->window_ms)};
    shard->redis->command(set, [this, shard, rule_ptr, client_key, count, on_decided](RedisResult stored) {
      record_outcome(*shard, stored.ok);
      if (!stored.ok) {
        note_redis_error(stored.error);
        fall_back(*rule_ptr, client_key, "redis_error", on_decided);
        return;
      }
      on_decided(Decision{true, rule_ptr->limit - count - 1, 0});
    });
  });
}

// Hybrid: spend leased quota locally, and ask Redis only when it runs out.
// Redis never hands out more than the limit per fixed window, and leased tokens
// expire with their window, so the cluster stays within the limit. The cost is
// that quota leased by a node that then goes quiet is wasted until the window ends.
// (Leases are always fixed-window, whatever the rule's algorithm.)
void Decider::decide_hybrid(const Rule& rule, const std::string& client_key, DecideCb on_decided) {
  const std::string key = redis_key(rule, client_key);
  Lease& lease = leases_[key];
  const int64_t now = EventLoop::now_ms();
  const bool lease_is_current = now < lease.expires_at_ms;

  if (lease_is_current && lease.tokens > 0) {
    lease.tokens--;
    on_decided(Decision{true, lease.tokens, 0});  // remaining: what this node still holds
    return;
  }
  if (lease_is_current && lease.window_exhausted) {
    on_decided(Decision{false, 0, lease.expires_at_ms - now});
    return;
  }
  lease.waiting.push_back(std::move(on_decided));
  if (!lease.in_flight) request_lease(rule, client_key, key);
}

// Lease replies are {granted, ms_until_window_end}.
void Decider::request_lease(const Rule& rule, const std::string& client_key, const std::string& key) {
  Shard* shard = available_shard(key);
  if (!shard) {
    fall_back_waiting(rule, client_key, key, "breaker_open");
    return;
  }
  leases_[key].in_flight = true;
  const int64_t chunk = lease_chunk(rule, options_.nodes);
  const Rule* rule_ptr = &rule;
  shard->redis->eval("lease", key, {rule.limit, rule.window_ms, chunk},
                     [this, shard, rule_ptr, client_key, key, chunk](RedisResult result) {
                       record_outcome(*shard, result.ok);
                       Lease& lease = leases_[key];
                       lease.in_flight = false;
                       constexpr size_t kFields = 2;
                       if (!result.ok || result.values.size() != kFields) {
                         note_redis_error(describe_failure(result, kFields));
                         fall_back_waiting(*rule_ptr, client_key, key, "redis_error");
                         return;
                       }
                       const int64_t granted = result.values[0];
                       lease.tokens = granted;
                       lease.expires_at_ms = EventLoop::now_ms() + result.values[1];
                       lease.window_exhausted = granted < chunk;  // Redis gave us the last of the window
                       retry_waiting(*rule_ptr, client_key, key);
                     });
}

// Runs the queued requests through decide_hybrid again now that the lease has
// changed: each takes a token, is denied, or queues for the next lease.
void Decider::retry_waiting(const Rule& rule, const std::string& client_key, const std::string& key) {
  std::vector<DecideCb> waiting;
  waiting.swap(leases_[key].waiting);  // callbacks below may queue new requests for this key
  for (DecideCb& on_decided : waiting) decide_hybrid(rule, client_key, std::move(on_decided));
}

void Decider::fall_back_waiting(const Rule& rule, const std::string& client_key, const std::string& key,
                                const std::string& reason) {
  std::vector<DecideCb> waiting;
  waiting.swap(leases_[key].waiting);
  for (const DecideCb& on_decided : waiting) fall_back(rule, client_key, reason, on_decided);
}

void Decider::fall_back(const Rule& rule, const std::string& client_key, const std::string& reason,
                        const DecideCb& on_decided) {
  metrics_.inc("rl_fallback_total{reason=\"" + reason + "\"}");
  switch (rule.fail) {
    case FailMode::Open:
      on_decided(Decision{true, rule.limit, 0});  // nothing was counted, so report the full limit
      return;
    case FailMode::Closed:
      on_decided(Decision{false, 0, kFailClosedRetryMs});
      return;
    case FailMode::Local:
      on_decided(fallback_limiter(rule).allow(client_key, EventLoop::now_ms()));
      return;
  }
}

// Every failure is counted; the log line is throttled so an outage can't flood stderr.
void Decider::note_redis_error(const std::string& error) {
  metrics_.inc("rl_redis_errors_total");
  int64_t now = EventLoop::now_ms();
  if (now - last_error_log_ms_ < kErrorLogIntervalMs) return;
  last_error_log_ms_ = now;
  std::cerr << "redis: " << error << " (using the rule's fail mode)\n";
}

Decider::Shard* Decider::available_shard(const std::string& redis_key) {
  Shard& shard = shards_.at(ring_.node_for(redis_key));
  const bool allowed = shard.breaker.allow_request(EventLoop::now_ms());
  update_breaker_gauge(shard);  // allow_request may have moved it from open to half-open
  return allowed ? &shard : nullptr;
}

void Decider::record_outcome(Shard& shard, bool ok) {
  if (ok) {
    shard.breaker.on_success();
  } else {
    shard.breaker.on_failure(EventLoop::now_ms());
  }
  update_breaker_gauge(shard);
}

// 1 while the shard is not trusted (open or half-open), 0 when closed.
void Decider::update_breaker_gauge(const Shard& shard) {
  const bool trusted = shard.breaker.state() == BreakerState::Closed;
  metrics_.set(breaker_gauge(shard.redis->name()), trusted ? 0 : 1);
}

Limiter& Decider::local_limiter(const Rule& rule) {
  std::unique_ptr<Limiter>& limiter = local_limiters_[rule.name];
  if (!limiter) limiter = make_limiter(rule.algo, rule.limit, rule.window_ms);
  return *limiter;
}

Limiter& Decider::fallback_limiter(const Rule& rule) {
  std::unique_ptr<Limiter>& limiter = fallback_limiters_[rule.name];
  if (!limiter) {
    // Each node only sees about 1/N of the traffic, so it gets 1/N of the limit.
    int64_t share = std::max<int64_t>(1, rule.limit / options_.nodes);
    limiter = make_limiter(rule.algo, share, rule.window_ms);
  }
  return *limiter;
}
