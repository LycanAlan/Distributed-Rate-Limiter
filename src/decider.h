#pragma once
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "circuit_breaker.h"
#include "config.h"
#include "event_loop.h"
#include "hash_ring.h"
#include "metrics.h"
#include "redis_async.h"

// Decides allow/deny for one request, in the mode chosen by --mode:
//   local   in-process limiter per node; not shared, so N nodes admit N x the limit
//   redis   the rule's Lua script, run atomically inside Redis
//   naive   fixed window via GET then SET: two round trips, racy on purpose
//   hybrid  each node leases chunks of a fixed window's quota from Redis and
//           spends them locally: one Redis call per chunk, not per request
//
// Keys are spread over the --redis shards by a consistent hash ring. Each shard
// has a circuit breaker; while it is open, or when Redis can't answer, the
// rule's fail mode decides: open, closed, or a local limiter with limit / nodes.
//
// Local decisions complete synchronously; Redis ones when the reply arrives.
class Decider {
 public:
  using DecideCb = std::function<void(Decision decision)>;

  Decider(EventLoop& loop, const Options& options, Metrics& metrics);
  void decide(const Rule& rule, const std::string& client_key, DecideCb on_decided);

 private:
  struct Shard {
    std::unique_ptr<RedisShard> redis;
    CircuitBreaker breaker;
  };

  // Quota this node has leased for one key (hybrid mode).
  struct Lease {
    int64_t tokens = 0;              // requests this node may still admit without asking Redis
    int64_t expires_at_ms = 0;       // end of the window the tokens belong to (EventLoop clock)
    bool window_exhausted = false;   // Redis has handed out the whole window: deny until it ends
    bool in_flight = false;          // a lease request is out; new requests wait for it
    std::vector<DecideCb> waiting;
  };

  void decide_redis(const Rule& rule, const std::string& client_key, DecideCb on_decided);
  void decide_naive(const Rule& rule, const std::string& client_key, DecideCb on_decided);
  void decide_hybrid(const Rule& rule, const std::string& client_key, DecideCb on_decided);
  void request_lease(const Rule& rule, const std::string& client_key, const std::string& key);
  void retry_waiting(const Rule& rule, const std::string& client_key, const std::string& key);
  void fall_back_waiting(const Rule& rule, const std::string& client_key, const std::string& key,
                         const std::string& reason);

  void fall_back(const Rule& rule, const std::string& client_key, const std::string& reason,
                 const DecideCb& on_decided);
  void note_redis_error(const std::string& error);

  Shard* available_shard(const std::string& redis_key);  // nullptr while its breaker is open
  void record_outcome(Shard& shard, bool ok);
  void update_breaker_gauge(const Shard& shard);
  Limiter& local_limiter(const Rule& rule);     // full limit: --mode local
  Limiter& fallback_limiter(const Rule& rule);  // limit / nodes: Redis unavailable

  const Options& options_;
  Metrics& metrics_;
  const LuaScripts scripts_;  // shards hold a reference, so this is declared before shards_
  std::map<std::string, Shard> shards_;  // by name, "host:port"
  HashRing ring_;                        // of shard names
  std::unordered_map<std::string, std::unique_ptr<Limiter>> local_limiters_;     // by rule name
  std::unordered_map<std::string, std::unique_ptr<Limiter>> fallback_limiters_;  // by rule name
  std::unordered_map<std::string, Lease> leases_;                                // by Redis key
  int64_t last_error_log_ms_ = 0;
};
