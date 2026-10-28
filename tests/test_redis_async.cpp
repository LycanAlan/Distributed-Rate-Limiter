#include <gtest/gtest.h>
#include <hiredis/hiredis.h>

#include <optional>

#include "event_loop.h"
#include "redis_async.h"

// Integration tests against the live Redis on :6379. Skipped when it's down.

namespace {

constexpr int64_t kTimeoutMs = 50;
constexpr int64_t kReplyWaitMs = 2000;

const LuaScripts kScripts = {
    {"triple", "return {1, 2, 3}"},
    {"echo_args", "return {tonumber(ARGV[1]) + tonumber(ARGV[2]), #KEYS, KEYS[1] == 'rl:test:k' and 1 or 0}"},
    {"boom", "return redis.error_reply('boom')"},
};

}  // namespace

class RedisAsync : public ::testing::Test {
 protected:
  void SetUp() override {
    redisContext* c = redisConnect("127.0.0.1", 6379);
    bool redis_up = c && !c->err;
    if (c) redisFree(c);
    if (!redis_up) GTEST_SKIP() << "Redis not running on :6379";
  }

  // Starts one async call and runs the loop until its callback fires.
  RedisResult call(const std::function<void(RedisCb)>& start) {
    std::optional<RedisResult> result;
    uint64_t guard = loop.add_timer(kReplyWaitMs, [&] { loop.stop(); });
    start([&](RedisResult r) {
      result = std::move(r);
      loop.stop();
    });
    if (!result) loop.run();  // the callback may already have run synchronously
    loop.cancel_timer(guard);
    if (!result) {
      ADD_FAILURE() << "no reply within " << kReplyWaitMs << " ms";
      return {};
    }
    return *result;
  }

  void run_loop_for(int64_t ms) {
    loop.add_timer(ms, [&] { loop.stop(); });
    loop.run();
  }

  static void send_sync(const char* command) {
    redisContext* c = redisConnect("127.0.0.1", 6379);
    freeReplyObject(redisCommand(c, command));
    redisFree(c);
  }

  EventLoop loop;
};

TEST_F(RedisAsync, EvalReturnsTheScriptResult) {
  RedisShard shard(loop, "127.0.0.1", 6379, kTimeoutMs, kScripts);
  shard.connect();  // eval right away: commands queue until the connection is up
  RedisResult r = call([&](RedisCb cb) { shard.eval("triple", "rl:test:k", {}, cb); });
  EXPECT_TRUE(r.ok) << r.error;
  EXPECT_EQ(r.values, (std::vector<long long>{1, 2, 3}));
  EXPECT_TRUE(shard.connected());
  EXPECT_EQ(shard.name(), "127.0.0.1:6379");
}

TEST_F(RedisAsync, EvalPassesKeyAndArgs) {
  RedisShard shard(loop, "127.0.0.1", 6379, kTimeoutMs, kScripts);
  shard.connect();
  RedisResult r = call([&](RedisCb cb) { shard.eval("echo_args", "rl:test:k", {2, 3}, cb); });
  EXPECT_TRUE(r.ok) << r.error;
  EXPECT_EQ(r.values, (std::vector<long long>{5, 1, 1}));
}

TEST_F(RedisAsync, ScriptErrorsAreReported) {
  RedisShard shard(loop, "127.0.0.1", 6379, kTimeoutMs, kScripts);
  shard.connect();
  RedisResult r = call([&](RedisCb cb) { shard.eval("boom", "rl:test:k", {}, cb); });
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.error.find("boom"), std::string::npos) << r.error;
}

TEST_F(RedisAsync, UnknownScriptNameIsAnError) {
  RedisShard shard(loop, "127.0.0.1", 6379, kTimeoutMs, kScripts);
  shard.connect();
  RedisResult r = call([&](RedisCb cb) { shard.eval("no_such_script", "rl:test:k", {}, cb); });
  EXPECT_FALSE(r.ok);
}

TEST_F(RedisAsync, SurvivesScriptCacheFlush) {
  // After SCRIPT FLUSH (or a Redis restart) EVALSHA answers NOSCRIPT; the
  // client must fall back to EVAL with the source instead of failing.
  RedisShard shard(loop, "127.0.0.1", 6379, kTimeoutMs, kScripts);
  shard.connect();
  ASSERT_TRUE(call([&](RedisCb cb) { shard.eval("triple", "rl:test:k", {}, cb); }).ok);
  ASSERT_TRUE(call([&](RedisCb cb) { shard.command({"SCRIPT", "FLUSH"}, cb); }).ok);
  RedisResult r = call([&](RedisCb cb) { shard.eval("triple", "rl:test:k", {}, cb); });
  EXPECT_TRUE(r.ok) << r.error;
  EXPECT_EQ(r.values, (std::vector<long long>{1, 2, 3}));
}

TEST_F(RedisAsync, CommandsReturnIntegersAndText) {
  RedisShard shard(loop, "127.0.0.1", 6379, kTimeoutMs, kScripts);
  shard.connect();
  call([&](RedisCb cb) { shard.command({"DEL", "rl:test:n"}, cb); });

  RedisResult set = call([&](RedisCb cb) { shard.command({"SET", "rl:test:n", "41"}, cb); });
  EXPECT_TRUE(set.ok);
  EXPECT_EQ(set.text, "OK");
  EXPECT_EQ(call([&](RedisCb cb) { shard.command({"INCR", "rl:test:n"}, cb); }).values,
            (std::vector<long long>{42}));
  EXPECT_EQ(call([&](RedisCb cb) { shard.command({"GET", "rl:test:n"}, cb); }).values,
            (std::vector<long long>{42}));  // numeric strings come back as numbers
  RedisResult missing = call([&](RedisCb cb) { shard.command({"GET", "rl:test:missing"}, cb); });
  EXPECT_TRUE(missing.ok);
  EXPECT_TRUE(missing.values.empty());

  call([&](RedisCb cb) { shard.command({"DEL", "rl:test:n"}, cb); });
}

TEST_F(RedisAsync, StalledRedisTimesOutFastThenRecovers) {
  RedisShard shard(loop, "127.0.0.1", 6379, kTimeoutMs, kScripts);
  shard.connect();
  ASSERT_EQ(call([&](RedisCb cb) { shard.command({"PING"}, cb); }).text, "PONG");

  send_sync("CLIENT PAUSE 300 ALL");  // Redis stops answering everyone for 300 ms
  int64_t started = EventLoop::now_ms();
  RedisResult stalled = call([&](RedisCb cb) { shard.eval("triple", "rl:test:k", {}, cb); });
  int64_t waited = EventLoop::now_ms() - started;
  EXPECT_FALSE(stalled.ok);
  EXPECT_LT(waited, 150) << "a 50 ms timeout took " << waited << " ms";

  run_loop_for(900);  // pause over, reconnect (every 500 ms) has happened
  RedisResult recovered = call([&](RedisCb cb) { shard.eval("triple", "rl:test:k", {}, cb); });
  EXPECT_TRUE(recovered.ok) << recovered.error;
  EXPECT_TRUE(shard.connected());
}

TEST_F(RedisAsync, UnreachableRedisFailsFast) {
  RedisShard shard(loop, "127.0.0.1", 1, kTimeoutMs, kScripts);  // nothing listens on port 1
  shard.connect();
  int64_t started = EventLoop::now_ms();
  RedisResult r = call([&](RedisCb cb) { shard.eval("triple", "rl:test:k", {}, cb); });
  EXPECT_FALSE(r.ok);
  EXPECT_LT(EventLoop::now_ms() - started, 200);
  EXPECT_FALSE(shard.connected());
}

TEST_F(RedisAsync, DestroyingTheShardWithCommandsInFlightIsSafe) {
  bool callback_ran = false;
  {
    RedisShard shard(loop, "127.0.0.1", 6379, kTimeoutMs, kScripts);
    shard.connect();
    shard.eval("triple", "rl:test:k", {}, [&](RedisResult) { callback_ran = true; });
  }  // destroyed before the loop ever ran: the reply must not call back into dead code
  run_loop_for(20);
  EXPECT_FALSE(callback_ran);
}
