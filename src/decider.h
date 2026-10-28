#pragma once
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "config.h"
#include "event_loop.h"
#include "metrics.h"

// Decides allow/deny for one request. Completes synchronously in local mode;
// the Redis-backed modes (tasks C8, C9) complete when Redis replies.
class Decider {
 public:
  using DecideCb = std::function<void(Decision decision)>;

  Decider(EventLoop& loop, const Options& options, Metrics& metrics);
  void decide(const Rule& rule, const std::string& client_key, DecideCb on_decided);

 private:
  Limiter& local_limiter(const Rule& rule);  // created on first use, one per rule

  EventLoop& loop_;
  const Options& options_;
  Metrics& metrics_;
  std::unordered_map<std::string, std::unique_ptr<Limiter>> local_limiters_;  // by rule name
};
