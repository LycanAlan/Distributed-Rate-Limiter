#include "limiters.h"

// TODO(Ali): implement all four. Tests: tests/test_limiters.cpp
// Hints: docs/plans/2026-10-26-rate-limiter-plan.md, Task U1.
// These stubs deny everything so every test starts red.

FixedWindowLimiter::FixedWindowLimiter(int64_t limit, int64_t window_ms)
    : limit_(limit), window_ms_(window_ms) {}

Decision FixedWindowLimiter::allow(const std::string&, int64_t) { return {false, 0, 0}; }

SlidingLogLimiter::SlidingLogLimiter(int64_t limit, int64_t window_ms)
    : limit_(limit), window_ms_(window_ms) {}

Decision SlidingLogLimiter::allow(const std::string&, int64_t) { return {false, 0, 0}; }

SlidingCounterLimiter::SlidingCounterLimiter(int64_t limit, int64_t window_ms)
    : limit_(limit), window_ms_(window_ms) {}

Decision SlidingCounterLimiter::allow(const std::string&, int64_t) { return {false, 0, 0}; }

TokenBucketLimiter::TokenBucketLimiter(int64_t limit, int64_t window_ms)
    : limit_(limit), window_ms_(window_ms) {}

Decision TokenBucketLimiter::allow(const std::string&, int64_t) { return {false, 0, 0}; }
