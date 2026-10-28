#include "decider.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <optional>
#include <stdexcept>

namespace {

constexpr int64_t kFailClosedRetryMs = 1000;
constexpr int64_t kErrorLogIntervalMs = 1000;

const std::vector<std::string> kScriptNames = {"fixed_window", "sliding_log", "sliding_counter", "token_bucket",
                                               "lease"};

LuaScripts scripts_for(const Options& options) {
  bool uses_lua = options.mode == Mode::Redis || options.mode == Mode::Hybrid;
  return uses_lua ? load_lua_scripts(options.lua_dir, kScriptNames) : LuaScripts{};
}

std::string redis_key(const Rule& rule, const std::string& client_key) {
  return "rl:" + rule.name + ":" + client_key;
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

std::string describe_failure(const RedisResult& result) {
  if (!result.ok) return result.error;
  return "script returned " + std::to_string(result.values.size()) + " values, expected 3";
}

}  // namespace

Decider::Decider(EventLoop& loop, const Options& options, Metrics& metrics)
    : options_(options), metrics_(metrics), scripts_(scripts_for(options)) {
  if (options_.mode == Mode::Hybrid) throw std::runtime_error("--mode hybrid is not built yet (plan task C9)");
  if (options_.mode == Mode::Local) return;
  for (const auto& [host, port] : options_.redis) {
    shards_.push_back(std::make_unique<RedisShard>(loop, host, port, options_.redis_timeout_ms, scripts_));
    shards_.back()->connect();
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
      return;  // rejected in the constructor until task C9
  }
}

void Decider::decide_redis(const Rule& rule, const std::string& client_key, DecideCb on_decided) {
  const std::string key = redis_key(rule, client_key);
  const Rule* rule_ptr = &rule;  // rules live in the Server for the whole run
  shard_for(key).eval(algo_name(rule.algo), key, {rule.limit, rule.window_ms},
                      [this, rule_ptr, client_key, on_decided = std::move(on_decided)](RedisResult result) {
                        std::optional<Decision> decision = decision_from_script(result);
                        if (!decision) {
                          note_redis_error(describe_failure(result));
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
  RedisShard* shard = &shard_for(base_key);
  const Rule* rule_ptr = &rule;

  shard->command({"GET", key}, [this, shard, rule_ptr, client_key, key, retry_after_ms,
                                on_decided = std::move(on_decided)](RedisResult got) {
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
    shard->command(set, [this, rule_ptr, client_key, count, on_decided](RedisResult stored) {
      if (!stored.ok) {
        note_redis_error(stored.error);
        fall_back(*rule_ptr, client_key, "redis_error", on_decided);
        return;
      }
      on_decided(Decision{true, rule_ptr->limit - count - 1, 0});
    });
  });
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

// ponytail: one shard until task C9 puts the hash ring in front of all of them.
RedisShard& Decider::shard_for(const std::string&) { return *shards_.front(); }

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
