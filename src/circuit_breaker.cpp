#include "circuit_breaker.h"

// TODO(Ali): implement. Tests: tests/test_breaker.cpp
// Hints: docs/plans/2026-10-26-rate-limiter-plan.md, Task U4.

CircuitBreaker::CircuitBreaker(int, int64_t) {}

bool CircuitBreaker::allow_request(int64_t) { return false; }

void CircuitBreaker::on_success() {}

void CircuitBreaker::on_failure(int64_t) {}

BreakerState CircuitBreaker::state() const { return BreakerState::Open; }
