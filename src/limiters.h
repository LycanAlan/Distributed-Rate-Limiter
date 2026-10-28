#pragma once
#include <cstdint>
#include <deque>
#include <string>
#include <unordered_map>

#include "types.h"

// All four take (limit, window_ms). now_ms comes from the caller, so tests can
// drive a fake clock. Exact semantics: docs/specs/2026-10-26-rate-limiter-design.md §5.2
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
  int64_t limit_, window_ms_;
  // TODO(Ali): per-key state
};

class SlidingLogLimiter : public Limiter {
 public:
  SlidingLogLimiter(int64_t limit, int64_t window_ms);
  Decision allow(const std::string& key, int64_t now_ms) override;

 private:
  int64_t limit_, window_ms_;
  // TODO(Ali): per-key state
};

class SlidingCounterLimiter : public Limiter {
 public:
  SlidingCounterLimiter(int64_t limit, int64_t window_ms);
  Decision allow(const std::string& key, int64_t now_ms) override;

 private:
  int64_t limit_, window_ms_;
  // TODO(Ali): per-key state
};

// capacity = limit, refill = limit tokens per window_ms, starts full.
class TokenBucketLimiter : public Limiter {
 public:
  TokenBucketLimiter(int64_t limit, int64_t window_ms);
  Decision allow(const std::string& key, int64_t now_ms) override;

 private:
  int64_t limit_, window_ms_;
  // TODO(Ali): per-key state
};
