#pragma once
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "config.h"
#include "event_loop.h"
#include "metrics.h"
#include "redis_async.h"

// Decides allow/deny for one request, in the mode chosen by --mode:
//   local   in-process limiter per node; not shared, so N nodes admit N x the limit
//   redis   the rule's Lua script, run atomically inside Redis
//   naive   fixed window via GET then SET: two round trips, racy on purpose
//   hybrid  leased quota (task C9)
// When Redis can't answer, the rule's fail mode decides: open, closed, or a
// local limiter with limit / nodes.
//
// Local decisions complete synchronously; Redis ones when the reply arrives.
class Decider {
 public:
  using DecideCb = std::function<void(Decision decision)>;

  Decider(EventLoop& loop, const Options& options, Metrics& metrics);
  void decide(const Rule& rule, const std::string& client_key, DecideCb on_decided);

 private:
  void decide_redis(const Rule& rule, const std::string& client_key, DecideCb on_decided);
  void decide_naive(const Rule& rule, const std::string& client_key, DecideCb on_decided);
  void fall_back(const Rule& rule, const std::string& client_key, const std::string& reason,
                 const DecideCb& on_decided);
  void note_redis_error(const std::string& error);

  RedisShard& shard_for(const std::string& redis_key);
  Limiter& local_limiter(const Rule& rule);     // full limit: --mode local
  Limiter& fallback_limiter(const Rule& rule);  // limit / nodes: Redis unavailable

  const Options& options_;
  Metrics& metrics_;
  const LuaScripts scripts_;  // shards hold a reference, so this is declared before shards_
  std::vector<std::unique_ptr<RedisShard>> shards_;
  std::unordered_map<std::string, std::unique_ptr<Limiter>> local_limiters_;     // by rule name
  std::unordered_map<std::string, std::unique_ptr<Limiter>> fallback_limiters_;  // by rule name
  int64_t last_error_log_ms_ = 0;
};
