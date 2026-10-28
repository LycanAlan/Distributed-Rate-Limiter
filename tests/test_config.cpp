#include <gtest/gtest.h>

#include <sstream>

#include "config.h"
#include "metrics.h"

static std::vector<Rule> rules_from(const std::string& text) {
  std::istringstream in(text);
  return parse_rules(in);
}

static std::string error_of(const std::string& text) {
  try {
    rules_from(text);
  } catch (const std::runtime_error& e) {
    return e.what();
  }
  return "";
}

TEST(Config, ParsesSpecExampleRules) {
  auto rules = rules_from(
      "# name  method path_prefix key_by            algo             limit  window_ms  fail\n"
      "\n"
      "login   POST   /login      ip                token_bucket     5      60000      closed\n"
      "api     *      /api        header:X-User-Id  sliding_counter  100    60000      local\n");
  ASSERT_EQ(rules.size(), 2u);
  EXPECT_EQ(rules[0].name, "login");
  EXPECT_EQ(rules[0].method, "POST");
  EXPECT_EQ(rules[0].path_prefix, "/login");
  EXPECT_EQ(rules[0].key_by, "ip");
  EXPECT_EQ(rules[0].algo, Algo::TokenBucket);
  EXPECT_EQ(rules[0].limit, 5);
  EXPECT_EQ(rules[0].window_ms, 60000);
  EXPECT_EQ(rules[0].fail, FailMode::Closed);
  EXPECT_EQ(rules[1].method, "*");
  EXPECT_EQ(rules[1].key_by, "header:X-User-Id");
  EXPECT_EQ(rules[1].algo, Algo::SlidingCounter);
  EXPECT_EQ(rules[1].fail, FailMode::Local);
}

TEST(Config, ErrorsNameTheLine) {
  std::string err = error_of(
      "# comment\n"
      "ok * /a ip fixed_window 1 1000 open\n"
      "bad * /b ip leaky_bucket 1 1000 open\n");
  EXPECT_NE(err.find("line 3"), std::string::npos) << err;
  EXPECT_NE(err.find("leaky_bucket"), std::string::npos) << err;
}

TEST(Config, RejectsBadFields) {
  for (std::string line : {"r * /a ip fixed_window 1 1000",                 // 7 columns
                           "r * /a ip fixed_window 0 1000 open",            // limit 0
                           "r * /a ip fixed_window 5 -1 open",              // window < 1
                           "r * /a ip fixed_window 5x 1000 open",           // not a number
                           "r * /a cookie fixed_window 1 1000 open",        // bad key_by
                           "r * /a header: fixed_window 1 1000 open",       // empty header name
                           "r * /a ip fixed_window 1 1000 maybe",           // bad fail mode
                           "r * api ip fixed_window 1 1000 open",           // prefix without '/'
                           "r get /a ip fixed_window 1 1000 open"}) {       // lowercase method
    EXPECT_NE(error_of(line + "\n"), "") << line;
  }
  EXPECT_NE(error_of("r * /a ip fixed_window 1 1000 open\nr * /b ip fixed_window 1 1000 open\n"), "")
      << "duplicate rule names";
}

TEST(Config, MatchRuleUsesMethodAndPathSegments) {
  auto rules = rules_from(
      "login POST /login ip fixed_window 5 1000 closed\n"
      "api   *    /api   ip fixed_window 5 1000 open\n"
      "all   *    /      ip fixed_window 5 1000 open\n");
  EXPECT_EQ(match_rule(rules, "POST", "/login")->name, "login");
  EXPECT_EQ(match_rule(rules, "GET", "/login")->name, "all");  // method mismatch falls through
  EXPECT_EQ(match_rule(rules, "GET", "/api")->name, "api");
  EXPECT_EQ(match_rule(rules, "GET", "/api/v1/x?q=1")->name, "api");
  EXPECT_EQ(match_rule(rules, "GET", "/api?q=1")->name, "api");  // query is not part of the path
  EXPECT_EQ(match_rule(rules, "GET", "/apix")->name, "all");     // prefix matches whole segments
  EXPECT_EQ(match_rule(std::vector<Rule>(rules.begin(), rules.begin() + 2), "GET", "/other"), nullptr);
}

TEST(Config, ParseArgsGatewayExample) {
  const char* argv[] = {"gateway", "--port", "8081", "--upstream", "127.0.0.1:9000", "--rules",
                        "rules.conf", "--mode", "redis", "--redis", "127.0.0.1:6379,127.0.0.1:6380",
                        "--nodes", "3"};
  Options o = parse_args(13, argv);
  EXPECT_EQ(o.port, 8081);
  EXPECT_FALSE(o.serve_upstream);
  EXPECT_EQ(o.upstream_host, "127.0.0.1");
  EXPECT_EQ(o.upstream_port, 9000);
  EXPECT_EQ(o.rules_path, "rules.conf");
  EXPECT_EQ(o.mode, Mode::Redis);
  ASSERT_EQ(o.redis.size(), 2u);
  EXPECT_EQ(o.redis[1], (std::pair<std::string, int>{"127.0.0.1", 6380}));
  EXPECT_EQ(o.nodes, 3);
  EXPECT_EQ(o.redis_timeout_ms, 50);
}

TEST(Config, LuaDirDefaultsToTheRepoAndCanBeOverridden) {
  const char* defaults[] = {"gateway"};
  EXPECT_EQ(parse_args(1, defaults).lua_dir, LUA_DIR);
  const char* custom[] = {"gateway", "--lua-dir", "/tmp/scripts"};
  EXPECT_EQ(parse_args(3, custom).lua_dir, "/tmp/scripts");
}

TEST(Config, ParseArgsUpstreamExample) {
  const char* argv[] = {"gateway", "--serve-upstream", "--port", "9000"};
  Options o = parse_args(4, argv);
  EXPECT_TRUE(o.serve_upstream);
  EXPECT_EQ(o.port, 9000);
}

TEST(Config, ParseArgsRejectsNonsense) {
  const char* unknown[] = {"gateway", "--colour", "red"};
  EXPECT_THROW(parse_args(3, unknown), std::runtime_error);
  const char* missing[] = {"gateway", "--port"};
  EXPECT_THROW(parse_args(2, missing), std::runtime_error);
  const char* mode[] = {"gateway", "--mode", "psychic"};
  EXPECT_THROW(parse_args(3, mode), std::runtime_error);
  const char* port[] = {"gateway", "--port", "70000"};
  EXPECT_THROW(parse_args(3, port), std::runtime_error);
  const char* hostport[] = {"gateway", "--upstream", "nohost"};
  EXPECT_THROW(parse_args(3, hostport), std::runtime_error);
}

TEST(Config, AlgoNamesMatchLuaScripts) {
  EXPECT_STREQ(algo_name(Algo::FixedWindow), "fixed_window");
  EXPECT_STREQ(algo_name(Algo::SlidingLog), "sliding_log");
  EXPECT_STREQ(algo_name(Algo::SlidingCounter), "sliding_counter");
  EXPECT_STREQ(algo_name(Algo::TokenBucket), "token_bucket");
  for (Algo a : {Algo::FixedWindow, Algo::SlidingLog, Algo::SlidingCounter, Algo::TokenBucket})
    EXPECT_NE(make_limiter(a, 1, 1000), nullptr);
}

TEST(Metrics, RendersCountersGaugesAndCumulativeHistogram) {
  Metrics m;
  m.inc("rl_requests_total{decision=\"allowed\"}");
  m.inc("rl_requests_total{decision=\"allowed\"}", 2);
  m.inc("rl_requests_total{decision=\"denied\"}");
  m.set("rl_breaker_open{shard=\"127.0.0.1:6380\"}", 1);
  m.observe_latency_us(30);
  m.observe_latency_us(700);
  m.observe_latency_us(100000);
  EXPECT_EQ(m.get("rl_requests_total{decision=\"allowed\"}"), 3);
  EXPECT_EQ(m.get("never_touched"), 0);

  std::string out = m.render();
  for (const char* line : {"# TYPE rl_requests_total counter\n",
                           "rl_requests_total{decision=\"allowed\"} 3\n",
                           "rl_requests_total{decision=\"denied\"} 1\n",
                           "# TYPE rl_breaker_open gauge\n",
                           "rl_breaker_open{shard=\"127.0.0.1:6380\"} 1\n",
                           "# TYPE rl_decision_latency_us histogram\n",
                           "rl_decision_latency_us_bucket{le=\"50\"} 1\n",
                           "rl_decision_latency_us_bucket{le=\"500\"} 1\n",
                           "rl_decision_latency_us_bucket{le=\"1000\"} 2\n",
                           "rl_decision_latency_us_bucket{le=\"50000\"} 2\n",
                           "rl_decision_latency_us_bucket{le=\"+Inf\"} 3\n",
                           "rl_decision_latency_us_sum 100730\n",
                           "rl_decision_latency_us_count 3\n"}) {
    EXPECT_NE(out.find(line), std::string::npos) << "missing: " << line << "in:\n" << out;
  }
  // one TYPE line per family, not per series
  EXPECT_EQ(out.find("# TYPE rl_requests_total"), out.rfind("# TYPE rl_requests_total"));
}
