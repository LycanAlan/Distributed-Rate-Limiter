#pragma once
#include <cstdint>

enum class BreakerState { Closed, Open, HalfOpen };

// Closed -> Open after `failure_threshold` consecutive failures.
// Open -> HalfOpen once `open_ms` has passed (one probe allowed).
// HalfOpen -> Closed on probe success, -> Open (fresh timer) on probe failure.
// Spec §5.5.
class CircuitBreaker {
 public:
  CircuitBreaker(int failure_threshold, int64_t open_ms);
  bool allow_request(int64_t now_ms);
  void on_success();
  void on_failure(int64_t now_ms);
  BreakerState state() const;

 private:
  // TODO(Ali)
};
