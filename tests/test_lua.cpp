#include <gtest/gtest.h>
#include <hiredis/hiredis.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>

// Integration tests for lua/*.lua against a live Redis on :6379, database 15.
// Skipped (not failed) when Redis is down.

static std::string load(const std::string& name) {
  std::ifstream f(std::string(LUA_DIR) + "/" + name);
  std::stringstream ss;
  ss << f.rdbuf();
  return ss.str();
}
static void sleep_ms(long long ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

class Lua : public ::testing::Test {
 protected:
  redisContext* c = nullptr;

  void SetUp() override {
    c = redisConnect("127.0.0.1", 6379);
    if (!c || c->err) GTEST_SKIP() << "Redis not running on :6379";
    freeReplyObject(redisCommand(c, "SELECT 15"));
    freeReplyObject(redisCommand(c, "FLUSHDB"));
  }
  void TearDown() override {
    if (c) redisFree(c);
  }

  // Runs a script, returns its integer array. On a Redis error the test fails
  // and {-1, -1, -1} comes back, so callers can index without crashing.
  std::vector<long long> run(const std::string& file, const std::string& key, long long limit,
                             long long window, long long chunk = -1) {
    std::string src = load(file);
    redisReply* r =
        chunk < 0 ? (redisReply*)redisCommand(c, "EVAL %s 1 %s %lld %lld", src.c_str(),
                                              key.c_str(), limit, window)
                  : (redisReply*)redisCommand(c, "EVAL %s 1 %s %lld %lld %lld", src.c_str(),
                                              key.c_str(), limit, window, chunk);
    const std::vector<long long> failed{-1, -1, -1};
    EXPECT_NE(r, nullptr);
    if (!r) return failed;
    if (r->type != REDIS_REPLY_ARRAY) {
      ADD_FAILURE() << file << ": " << (r->type == REDIS_REPLY_ERROR ? r->str : "reply is not an array");
      freeReplyObject(r);
      return failed;
    }
    std::vector<long long> out;
    for (size_t i = 0; i < r->elements; i++) {
      EXPECT_EQ(r->element[i]->type, REDIS_REPLY_INTEGER) << "element " << i << " must be an integer";
      out.push_back(r->element[i]->integer);
    }
    freeReplyObject(r);
    if (out.empty()) {
      ADD_FAILURE() << file << ": empty reply";
      return failed;
    }
    return out;
  }

  void expect_all_keys_expire() {
    redisReply* keys = (redisReply*)redisCommand(c, "KEYS *");
    ASSERT_GT(keys->elements, 0u);
    for (size_t i = 0; i < keys->elements; i++) {
      redisReply* t = (redisReply*)redisCommand(c, "PTTL %s", keys->element[i]->str);
      EXPECT_GT(t->integer, 0) << keys->element[i]->str << " has no TTL";
      freeReplyObject(t);
    }
    freeReplyObject(keys);
  }

  // Windows are epoch-aligned. Sleep until the current window has more than
  // half its length left, so a quick burst of calls can't straddle a boundary.
  void wait_for_fresh_window(long long window_ms) {
    for (;;) {
      redisReply* t = (redisReply*)redisCommand(c, "TIME");
      long long now = atoll(t->element[0]->str) * 1000 + atoll(t->element[1]->str) / 1000;
      freeReplyObject(t);
      long long left = window_ms - now % window_ms;
      if (left > window_ms / 2) return;
      sleep_ms(left + 1);
    }
  }
};

TEST_F(Lua, FixedWindowLimitsAndExpires) {
  wait_for_fresh_window(300);
  for (int i = 0; i < 3; i++) {
    auto r = run("fixed_window.lua", "rl:t:u", 3, 300);
    ASSERT_EQ(r.size(), 3u);
    EXPECT_EQ(r[0], 1);
    EXPECT_EQ(r[1], 2 - i);
  }
  auto d = run("fixed_window.lua", "rl:t:u", 3, 300);
  ASSERT_EQ(d.size(), 3u);
  EXPECT_EQ(d[0], 0);
  EXPECT_GT(d[2], 0);
  EXPECT_LE(d[2], 300);
  expect_all_keys_expire();
  sleep_ms(d[2] + 20);
  EXPECT_EQ(run("fixed_window.lua", "rl:t:u", 3, 300)[0], 1);
}

TEST_F(Lua, SlidingLogCountsSameMillisecondRequests) {
  for (int i = 0; i < 3; i++) EXPECT_EQ(run("sliding_log.lua", "rl:t:u", 3, 300)[0], 1);
  auto d = run("sliding_log.lua", "rl:t:u", 3, 300);
  ASSERT_EQ(d.size(), 3u);
  EXPECT_EQ(d[0], 0);
  EXPECT_GT(d[2], 0);
  EXPECT_LE(d[2], 300);
  expect_all_keys_expire();
  sleep_ms(320);
  EXPECT_EQ(run("sliding_log.lua", "rl:t:u", 3, 300)[0], 1);
}

TEST_F(Lua, SlidingCounterLimitsAndRecovers) {
  wait_for_fresh_window(400);
  for (int i = 0; i < 4; i++) EXPECT_EQ(run("sliding_counter.lua", "rl:t:u", 4, 400)[0], 1);
  auto d = run("sliding_counter.lua", "rl:t:u", 4, 400);
  ASSERT_EQ(d.size(), 3u);
  EXPECT_EQ(d[0], 0);
  EXPECT_GT(d[2], 0);
  EXPECT_LE(d[2], 401);
  expect_all_keys_expire();
  sleep_ms(850);  // two full windows later the old count has no weight
  EXPECT_EQ(run("sliding_counter.lua", "rl:t:u", 4, 400)[0], 1);
}

TEST_F(Lua, TokenBucketRefills) {
  for (int i = 0; i < 4; i++) EXPECT_EQ(run("token_bucket.lua", "rl:t:u", 4, 400)[0], 1);
  auto d = run("token_bucket.lua", "rl:t:u", 4, 400);  // 1 token per 100 ms
  ASSERT_EQ(d.size(), 3u);
  EXPECT_EQ(d[0], 0);
  EXPECT_GT(d[2], 0);
  EXPECT_LE(d[2], 101);
  expect_all_keys_expire();
  sleep_ms(120);
  EXPECT_EQ(run("token_bucket.lua", "rl:t:u", 4, 400)[0], 1);
  EXPECT_EQ(run("token_bucket.lua", "rl:t:u", 4, 400)[0], 0);
}

TEST_F(Lua, LeaseGrantsChunksUntilQuotaIsGone) {
  wait_for_fresh_window(1000);
  std::vector<long long> grants;
  for (int i = 0; i < 4; i++) {
    auto r = run("lease.lua", "rl:t:u", 10, 1000, 4);
    ASSERT_EQ(r.size(), 2u);
    grants.push_back(r[0]);
    EXPECT_GT(r[1], 0);
    EXPECT_LE(r[1], 1000);
  }
  EXPECT_EQ(grants, (std::vector<long long>{4, 4, 2, 0}));
  expect_all_keys_expire();
}

TEST_F(Lua, FixedWindowIsAtomicUnderConcurrency) {
  std::string src = load("fixed_window.lua");
  wait_for_fresh_window(5000);
  std::atomic<int> allowed{0};
  std::vector<std::thread> ts;
  for (int t = 0; t < 8; t++)
    ts.emplace_back([&] {
      redisContext* cc = redisConnect("127.0.0.1", 6379);
      freeReplyObject(redisCommand(cc, "SELECT 15"));
      for (int i = 0; i < 50; i++) {
        redisReply* r = (redisReply*)redisCommand(cc, "EVAL %s 1 rl:t:hot 100 5000", src.c_str());
        if (r && r->type == REDIS_REPLY_ARRAY && r->element[0]->integer == 1) allowed++;
        freeReplyObject(r);
      }
      redisFree(cc);
    });
  for (auto& t : ts) t.join();
  EXPECT_EQ(allowed.load(), 100);  // 400 attempts, exactly the limit gets through
}
