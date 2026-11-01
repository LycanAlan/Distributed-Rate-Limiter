#pragma once
#include <cstdint>
#include <deque>
#include <string>
#include <unordered_map>

#include "types.h"

// All four take (limit, window_ms). now_ms comes from the caller, so tests can
// drive a fake clock. Exact semantics: docs/specs/2026-10-26-rate-limiter-design.md §5.2
//
// ponytail: per-key state is never evicted, so memory grows with the number of
// distinct keys ever seen. Fine for a benchmark; add an idle-key sweep on a
// timer before keying on something unbounded like client IPs in production.
class Limiter {
 public:
  virtual ~Limiter() = default;
  virtual Decision allow(const std::string& key, int64_t now_ms) = 0;
};

class FixedWindowLimiter : public Limiter {
 public:
  FixedWindowLimiter(int64_t limit, int64_t window_ms);
  Decision allow(const std::string& key, int64_t now_ms) override;

 private:
  struct Window {
    int64_t index = 0;  // now_ms / window_ms of the window being counted
    int64_t count = 0;  // requests allowed in it
  };

  int64_t limit_, window_ms_;
  std::unordered_map<std::string, Window> windows_;
};

class SlidingLogLimiter : public Limiter {
 public:
  SlidingLogLimiter(int64_t limit, int64_t window_ms);
  Decision allow(const std::string& key, int64_t now_ms) override;

 private:
  int64_t limit_, window_ms_;
  std::unordered_map<std::string, std::deque<int64_t>> logs_;  // allowed timestamps, oldest first
};

class SlidingCounterLimiter : public Limiter {
 public:
  SlidingCounterLimiter(int64_t limit, int64_t window_ms);
  Decision allow(const std::string& key, int64_t now_ms) override;

 private:
  struct Counts {
    int64_t window = 0;    // index of the current fixed window
    int64_t current = 0;   // requests allowed in it
    int64_t previous = 0;  // requests allowed in the window before it
  };

  void roll_forward(Counts& counts, int64_t window) const;
  int64_t retry_after_ms(const Counts& counts, int64_t elapsed_ms) const;

  int64_t limit_, window_ms_;
  std::unordered_map<std::string, Counts> counts_;
};

// capacity = limit, refill = limit tokens per window_ms, starts full.
class TokenBucketLimiter : public Limiter {
 public:
  TokenBucketLimiter(int64_t limit, int64_t window_ms);
  Decision allow(const std::string& key, int64_t now_ms) override;

 private:
  struct Bucket {
    double tokens;    // fractional: refill adds a little every millisecond
    int64_t last_ms;  // when tokens was last brought up to date
  };

  void refill(Bucket& bucket, int64_t now_ms) const;

  int64_t limit_, window_ms_;
  std::unordered_map<std::string, Bucket> buckets_;
};
