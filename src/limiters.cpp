// The four rate-limiting algorithms, run in-process (one copy per gateway).
//
// Every one answers the same question: "may `key` make another request at
// now_ms, given at most `limit` requests per `window_ms`?" They differ in what
// they remember per key and in how they treat the edges of a window:
//
//   algorithm        state per key               accuracy at window edges
//   fixed window     window index + count        up to 2x the limit in a short burst
//   sliding log      one timestamp per request   exact
//   sliding counter  two counts                  close estimate, never a 2x burst
//   token bucket     tokens + last refill time   exact average rate, bursts up to `limit`
//
// The same algorithms run atomically inside Redis in lua/*.lua, which is what
// makes the limit hold across several gateways. Spec §5.2 has the exact rules.

#include "limiters.h"

#include <algorithm>
#include <cmath>

namespace {

Decision allowed(int64_t remaining) { return Decision{true, remaining, 0}; }
Decision denied(int64_t retry_after_ms) { return Decision{false, 0, retry_after_ms}; }

}  // namespace

// ---------------------------------------------------------------------------
// Fixed window
//
// Cut time into windows [0, W), [W, 2W), ... and count each key's requests in
// the current one. The count starts again from zero when a new window begins.
//
// Weakness: `limit` requests at 999 ms and `limit` more at 1000 ms all pass,
// because they land in different windows. That is 2x the limit in 1 ms.
// ---------------------------------------------------------------------------

FixedWindowLimiter::FixedWindowLimiter(int64_t limit, int64_t window_ms) : limit_(limit), window_ms_(window_ms) {}

Decision FixedWindowLimiter::allow(const std::string& key, int64_t now_ms) {
  const int64_t current_window = now_ms / window_ms_;
  Window& window = windows_[key];

  const bool window_has_moved_on = window.index != current_window;
  if (window_has_moved_on) {
    window.index = current_window;
    window.count = 0;
  }

  if (window.count >= limit_) {
    const int64_t window_end_ms = (current_window + 1) * window_ms_;
    return denied(window_end_ms - now_ms);
  }
  window.count++;
  return allowed(limit_ - window.count);
}

// ---------------------------------------------------------------------------
// Sliding log
//
// Remember the time of every allowed request. A request is allowed if fewer
// than `limit` of them fall inside the last W ms, i.e. in (now - W, now].
// The window really slides, so there is no boundary burst.
//
// Cost: up to `limit` timestamps per key. For 10,000 requests per hour that is
// 10,000 numbers per user, which is why the sliding counter exists.
// ---------------------------------------------------------------------------

SlidingLogLimiter::SlidingLogLimiter(int64_t limit, int64_t window_ms) : limit_(limit), window_ms_(window_ms) {}

Decision SlidingLogLimiter::allow(const std::string& key, int64_t now_ms) {
  std::deque<int64_t>& log = logs_[key];

  // Forget requests that have slid out of the window. The log is in time
  // order, so they are all at the front.
  const int64_t window_start_ms = now_ms - window_ms_;
  while (!log.empty() && log.front() <= window_start_ms) log.pop_front();

  const auto in_window = static_cast<int64_t>(log.size());
  if (in_window >= limit_) {
    // A slot frees up the moment the oldest request leaves the window.
    const int64_t oldest_leaves_at_ms = log.front() + window_ms_;
    return denied(oldest_leaves_at_ms - now_ms);
  }
  log.push_back(now_ms);
  return allowed(limit_ - (in_window + 1));
}

// ---------------------------------------------------------------------------
// Sliding window counter
//
// The fixed window's tiny state with most of the sliding log's accuracy.
// Keep counts for just two fixed windows, the current one and the one before,
// and assume the previous window's requests were spread evenly across it.
//
//       previous window          current window
//   |------------------------|------------------------|
//                  |<----- sliding window (W) ----->|
//                  ^ 70% of previous still overlaps   ^ now: 30% into current
//
//   estimate = previous * 0.7 + current
//
// In general the weight is (W - elapsed) / W. To stay in integers, both sides
// of "estimate < limit" are multiplied by W:
//
//   previous * (W - elapsed) + current * W  <  limit * W
// ---------------------------------------------------------------------------

SlidingCounterLimiter::SlidingCounterLimiter(int64_t limit, int64_t window_ms)
    : limit_(limit), window_ms_(window_ms) {}

Decision SlidingCounterLimiter::allow(const std::string& key, int64_t now_ms) {
  Counts& counts = counts_[key];
  roll_forward(counts, now_ms / window_ms_);

  const int64_t elapsed_ms = now_ms % window_ms_;  // how far into the current window we are
  const int64_t previous_weighted = counts.previous * (window_ms_ - elapsed_ms);
  const int64_t budget = limit_ * window_ms_;

  if (previous_weighted + counts.current * window_ms_ >= budget) {
    return denied(retry_after_ms(counts, elapsed_ms));
  }
  counts.current++;

  // Whole requests still under the estimate, rounded down.
  const int64_t room = budget - previous_weighted - counts.current * window_ms_;
  return allowed(std::max<int64_t>(0, room / window_ms_));
}

// Moves the counts to `window` if time has moved on since the last request.
void SlidingCounterLimiter::roll_forward(Counts& counts, int64_t window) const {
  if (window == counts.window + 1) {
    counts.previous = counts.current;  // the current window becomes the previous one
    counts.current = 0;
  } else if (window > counts.window + 1) {
    counts.previous = 0;  // a whole window passed with no requests at all
    counts.current = 0;
  }
  counts.window = window;
}

namespace {

// The earliest elapsed time e in [0, W] at which `fading * (W - e) < budget`:
// the moment a count that is fading out has faded enough to fit the budget.
//   fading * (W - e) < budget   <=>   W - e < budget / fading
//                               <=>   e >= W - ceil(budget / fading) + 1
int64_t first_moment_under_budget(int64_t fading, int64_t budget, int64_t window_ms) {
  if (fading == 0) return 0;  // nothing is fading, so the budget already fits
  const int64_t ceil_ratio = (budget + fading - 1) / fading;
  return std::clamp<int64_t>(window_ms - ceil_ratio + 1, 0, window_ms);
}

}  // namespace

// How long until a request would pass, if no more requests arrive meanwhile.
// There are two chances. Later in this window, as the previous window's weight
// fades. Or in the next window, where this window's count is the one fading.
int64_t SlidingCounterLimiter::retry_after_ms(const Counts& counts, int64_t elapsed_ms) const {
  const int64_t budget_left_this_window = (limit_ - counts.current) * window_ms_;
  if (budget_left_this_window > 0) {
    const int64_t at = first_moment_under_budget(counts.previous, budget_left_this_window, window_ms_);
    if (at < window_ms_) return at - elapsed_ms;
  }
  const int64_t into_next_window = first_moment_under_budget(counts.current, limit_ * window_ms_, window_ms_);
  return (window_ms_ - elapsed_ms) + into_next_window;
}

// ---------------------------------------------------------------------------
// Token bucket
//
// A bucket holds at most `limit` tokens and refills at limit / W tokens per ms.
// Each request takes one token; no token, no request. A full bucket lets a
// client burst `limit` requests at once, but over time the average rate can
// never exceed limit / W. This is what AWS API Gateway and Stripe use.
//
// Refill is lazy: no timer adds tokens. Each call first adds whatever accrued
// since the previous call, so an idle key costs no work at all.
// ---------------------------------------------------------------------------

TokenBucketLimiter::TokenBucketLimiter(int64_t limit, int64_t window_ms) : limit_(limit), window_ms_(window_ms) {}

Decision TokenBucketLimiter::allow(const std::string& key, int64_t now_ms) {
  const Bucket full{static_cast<double>(limit_), now_ms};
  Bucket& bucket = buckets_.try_emplace(key, full).first->second;  // a new key starts with a full bucket
  refill(bucket, now_ms);

  if (bucket.tokens < 1.0) {
    // Time to earn the missing part of a token. Multiplying before dividing
    // avoids the rounding error a precomputed rate (limit / W) would carry.
    const double missing = 1.0 - bucket.tokens;
    return denied(static_cast<int64_t>(std::ceil(missing * window_ms_ / limit_)));
  }
  bucket.tokens -= 1.0;
  return allowed(static_cast<int64_t>(bucket.tokens));  // whole tokens left, rounded down
}

void TokenBucketLimiter::refill(Bucket& bucket, int64_t now_ms) const {
  const int64_t elapsed_ms = now_ms - bucket.last_ms;
  const double earned = static_cast<double>(elapsed_ms) * limit_ / window_ms_;
  bucket.tokens = std::min(static_cast<double>(limit_), bucket.tokens + earned);  // never above capacity
  bucket.last_ms = now_ms;
}
