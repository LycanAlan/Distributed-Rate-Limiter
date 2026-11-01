// A circuit breaker stops a gateway from waiting on a Redis shard that is down.
//
// Without one, every request waits the full timeout (50 ms) for a shard that
// will not answer. With one, after a few failures in a row the gateway stops
// calling that shard ("open") and goes straight to the rule's fail mode.
// After open_ms it lets a single request through to see if the shard is back
// ("half-open").
//
//              failure_threshold failures in a row
//    Closed ─────────────────────────────────────────► Open
//      ▲                                              │   ▲
//      │ probe succeeds                  open_ms over │   │ probe fails
//      │                                              ▼   │
//      └──────────────────────────────────────────── HalfOpen
//                                         (one probe request at a time)

#include "circuit_breaker.h"

CircuitBreaker::CircuitBreaker(int failure_threshold, int64_t open_ms)
    : failure_threshold_(failure_threshold), open_ms_(open_ms) {}

bool CircuitBreaker::allow_request(int64_t now_ms) {
  switch (state_) {
    case BreakerState::Closed:
      return true;
    case BreakerState::HalfOpen:
      return false;  // the probe is still out; wait for its answer
    case BreakerState::Open:
      if (now_ms - opened_at_ms_ < open_ms_) return false;
      state_ = BreakerState::HalfOpen;  // cooled down: this request is the probe
      return true;
  }
  return false;
}

void CircuitBreaker::on_success() {
  // While open, a success can only be a late reply to a request sent before
  // the breaker opened. It says little about the shard now, so ignore it.
  if (state_ == BreakerState::Open) return;
  state_ = BreakerState::Closed;
  consecutive_failures_ = 0;
}

void CircuitBreaker::on_failure(int64_t now_ms) {
  switch (state_) {
    case BreakerState::Open:
      return;  // a late reply; restarting the timer here would keep the breaker open forever
    case BreakerState::HalfOpen:
      trip(now_ms);  // the probe failed: still down
      return;
    case BreakerState::Closed:
      consecutive_failures_++;
      if (consecutive_failures_ >= failure_threshold_) trip(now_ms);
      return;
  }
}

BreakerState CircuitBreaker::state() const { return state_; }

void CircuitBreaker::trip(int64_t now_ms) {
  state_ = BreakerState::Open;
  opened_at_ms_ = now_ms;
  consecutive_failures_ = 0;
}
