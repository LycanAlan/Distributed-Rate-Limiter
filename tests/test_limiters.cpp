#include <gtest/gtest.h>

#include "limiters.h"

// ---------- Fixed window ----------
TEST(FixedWindow, AllowsUpToLimitThenDenies) {
  FixedWindowLimiter l(3, 1000);
  for (int i = 0; i < 3; i++) {
    Decision d = l.allow("u", 100);
    EXPECT_TRUE(d.allowed) << "request " << i;
    EXPECT_EQ(d.remaining, 2 - i);
    EXPECT_EQ(d.retry_after_ms, 0);
  }
  Decision d = l.allow("u", 100);
  EXPECT_FALSE(d.allowed);
  EXPECT_EQ(d.remaining, 0);
  EXPECT_EQ(d.retry_after_ms, 900);  // window [0,1000) ends at 1000
}

TEST(FixedWindow, ResetsAtWindowBoundary) {
  FixedWindowLimiter l(2, 1000);
  l.allow("u", 998);
  l.allow("u", 999);
  EXPECT_FALSE(l.allow("u", 999).allowed);
  EXPECT_TRUE(l.allow("u", 1000).allowed);
}

TEST(FixedWindow, BoundaryBurstIsTheKnownWeakness) {
  FixedWindowLimiter l(5, 1000);
  int ok = 0;
  for (int i = 0; i < 5; i++) ok += l.allow("u", 999).allowed;
  for (int i = 0; i < 5; i++) ok += l.allow("u", 1000).allowed;
  EXPECT_EQ(ok, 10);  // 2x the limit in 1 ms: this is why sliding windows exist
}

TEST(FixedWindow, KeysAreIndependent) {
  FixedWindowLimiter l(1, 1000);
  EXPECT_TRUE(l.allow("a", 0).allowed);
  EXPECT_TRUE(l.allow("b", 0).allowed);
  EXPECT_FALSE(l.allow("a", 0).allowed);
}

// ---------- Sliding log ----------
TEST(SlidingLog, NoBoundaryBurst) {
  SlidingLogLimiter l(5, 1000);
  int ok = 0;
  for (int i = 0; i < 5; i++) ok += l.allow("u", 999).allowed;
  for (int i = 0; i < 5; i++) ok += l.allow("u", 1000).allowed;
  EXPECT_EQ(ok, 5);
}

TEST(SlidingLog, OldEntriesExpireExactly) {
  SlidingLogLimiter l(2, 1000);
  EXPECT_TRUE(l.allow("u", 0).allowed);
  EXPECT_TRUE(l.allow("u", 500).allowed);
  Decision d = l.allow("u", 999);
  EXPECT_FALSE(d.allowed);
  EXPECT_EQ(d.retry_after_ms, 1);           // entry at t=0 leaves at t=1000
  EXPECT_TRUE(l.allow("u", 1000).allowed);  // window is (now-1000, now]
  EXPECT_FALSE(l.allow("u", 1000).allowed);
}

TEST(SlidingLog, RemainingCountsDown) {
  SlidingLogLimiter l(3, 1000);
  EXPECT_EQ(l.allow("u", 0).remaining, 2);
  EXPECT_EQ(l.allow("u", 1).remaining, 1);
  EXPECT_EQ(l.allow("u", 2).remaining, 0);
  EXPECT_EQ(l.allow("u", 3).remaining, 0);
}

// ---------- Sliding window counter ----------
TEST(SlidingCounter, ActsLikeFixedWindowInFirstWindow) {
  SlidingCounterLimiter l(10, 1000);
  for (int i = 0; i < 10; i++) EXPECT_TRUE(l.allow("u", 100).allowed);
  EXPECT_FALSE(l.allow("u", 100).allowed);
}

TEST(SlidingCounter, WeightsPreviousWindow) {
  SlidingCounterLimiter l(10, 1000);
  for (int i = 0; i < 10; i++) l.allow("u", i);  // prev window full
  // t=1500: prev weight 0.5 -> estimate 5 + curr; 5 more fit
  int ok = 0;
  for (int i = 0; i < 10; i++) ok += l.allow("u", 1500).allowed;
  EXPECT_EQ(ok, 5);
}

TEST(SlidingCounter, NoBoundaryBurst) {
  SlidingCounterLimiter l(10, 1000);
  int ok = 0;
  for (int i = 0; i < 10; i++) ok += l.allow("u", 999).allowed;
  EXPECT_EQ(ok, 10);
  EXPECT_FALSE(l.allow("u", 1000).allowed);  // prev weight ~1.0
}

TEST(SlidingCounter, SkippedWindowsForgetOldCounts) {
  SlidingCounterLimiter l(10, 1000);
  for (int i = 0; i < 10; i++) l.allow("u", 0);
  int ok = 0;  // window 2: previous window (1) was empty
  for (int i = 0; i < 10; i++) ok += l.allow("u", 2500).allowed;
  EXPECT_EQ(ok, 10);
}

TEST(SlidingCounter, DeniedHasSaneRetryAfterAndFirstRemaining) {
  SlidingCounterLimiter l(10, 1000);
  EXPECT_EQ(l.allow("u", 0).remaining, 9);
  for (int i = 0; i < 9; i++) l.allow("u", 0);
  Decision d = l.allow("u", 0);
  EXPECT_FALSE(d.allowed);
  EXPECT_GT(d.retry_after_ms, 0);
  EXPECT_LE(d.retry_after_ms, 1001);  // exact answer here is 1001: at t=1000 prev still weighs 1.0
}

// ---------- Token bucket (capacity 10, refill 10 per 1000 ms) ----------
TEST(TokenBucket, StartsFullThenDenies) {
  TokenBucketLimiter l(10, 1000);
  EXPECT_EQ(l.allow("u", 0).remaining, 9);
  for (int i = 0; i < 9; i++) EXPECT_TRUE(l.allow("u", 0).allowed);
  Decision d = l.allow("u", 0);
  EXPECT_FALSE(d.allowed);
  EXPECT_NEAR(d.retry_after_ms, 100, 1);  // one token every 100 ms
}

TEST(TokenBucket, RefillsLazily) {
  TokenBucketLimiter l(10, 1000);
  for (int i = 0; i < 10; i++) l.allow("u", 0);
  EXPECT_FALSE(l.allow("u", 50).allowed);  // 0.5 token
  EXPECT_TRUE(l.allow("u", 100).allowed);  // 1 token
  EXPECT_FALSE(l.allow("u", 100).allowed);
}

TEST(TokenBucket, NeverExceedsCapacity) {
  TokenBucketLimiter l(10, 1000);
  l.allow("u", 0);
  int ok = 0;  // idle for a long time, burst is still capped at 10
  for (int i = 0; i < 20; i++) ok += l.allow("u", 1000000).allowed;
  EXPECT_EQ(ok, 10);
}
