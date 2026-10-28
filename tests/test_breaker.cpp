#include <gtest/gtest.h>

#include "circuit_breaker.h"

TEST(Breaker, StartsClosedAndAllows) {
  CircuitBreaker b(3, 1000);
  EXPECT_EQ(b.state(), BreakerState::Closed);
  EXPECT_TRUE(b.allow_request(0));
}

TEST(Breaker, OpensAfterThresholdConsecutiveFailures) {
  CircuitBreaker b(3, 1000);
  b.on_failure(10);
  b.on_failure(20);
  EXPECT_EQ(b.state(), BreakerState::Closed);
  b.on_failure(30);
  EXPECT_EQ(b.state(), BreakerState::Open);
  EXPECT_FALSE(b.allow_request(31));
}

TEST(Breaker, SuccessResetsTheFailureCount) {
  CircuitBreaker b(3, 1000);
  b.on_failure(1);
  b.on_failure(2);
  b.on_success();
  b.on_failure(3);
  b.on_failure(4);
  EXPECT_EQ(b.state(), BreakerState::Closed);
}

TEST(Breaker, StaysOpenUntilCooldownThenHalfOpens) {
  CircuitBreaker b(1, 1000);
  b.on_failure(100);
  EXPECT_FALSE(b.allow_request(1099));
  EXPECT_TRUE(b.allow_request(1100));
  EXPECT_EQ(b.state(), BreakerState::HalfOpen);
}

TEST(Breaker, HalfOpenAllowsExactlyOneProbe) {
  CircuitBreaker b(1, 1000);
  b.on_failure(0);
  EXPECT_TRUE(b.allow_request(1000));
  EXPECT_FALSE(b.allow_request(1001));
  EXPECT_FALSE(b.allow_request(5000));  // still waiting on the probe
}

TEST(Breaker, ProbeSuccessCloses) {
  CircuitBreaker b(1, 1000);
  b.on_failure(0);
  b.allow_request(1000);
  b.on_success();
  EXPECT_EQ(b.state(), BreakerState::Closed);
  EXPECT_TRUE(b.allow_request(1001));
}

TEST(Breaker, ProbeFailureReopensWithFreshTimer) {
  CircuitBreaker b(1, 1000);
  b.on_failure(0);
  b.allow_request(1000);
  b.on_failure(1000);
  EXPECT_EQ(b.state(), BreakerState::Open);
  EXPECT_FALSE(b.allow_request(1999));
  EXPECT_TRUE(b.allow_request(2000));
}

TEST(Breaker, LateRepliesWhileOpenAreIgnored) {
  CircuitBreaker b(1, 1000);
  b.on_failure(0);
  b.on_failure(900);  // a reply from a request sent before opening
  b.on_success();     // same
  EXPECT_EQ(b.state(), BreakerState::Open);
  EXPECT_TRUE(b.allow_request(1000));  // timer was not reset by the late failure
}
