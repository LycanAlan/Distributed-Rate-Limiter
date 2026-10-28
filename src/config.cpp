#include "config.h"

#include <cctype>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

namespace {

[[noreturn]] void fail(const std::string& message) { throw std::runtime_error(message); }

std::string quoted(const std::string& s) { return "'" + s + "'"; }

// Positive decimal integer: no sign, no spaces, no trailing junk.
bool parse_positive(const std::string& s, int64_t& out) {
  constexpr size_t kMaxDigits = 18;  // fits in int64_t
  if (s.empty() || s.size() > kMaxDigits) return false;
  int64_t value = 0;
  for (char c : s) {
    if (!std::isdigit((unsigned char)c)) return false;
    value = value * 10 + (c - '0');
  }
  if (value <= 0) return false;
  out = value;
  return true;
}

int64_t positive_or_fail(const std::string& s, const std::string& what) {
  int64_t value = 0;
  if (!parse_positive(s, value)) fail(what + " must be a positive integer, got " + quoted(s));
  return value;
}

int parse_port(const std::string& s) {
  constexpr int64_t kMaxPort = 65535;
  int64_t port = 0;
  if (!parse_positive(s, port) || port > kMaxPort) fail("bad port " + quoted(s));
  return static_cast<int>(port);
}

std::pair<std::string, int> parse_host_port(const std::string& s) {
  size_t colon = s.rfind(':');
  if (colon == std::string::npos || colon == 0) fail("expected host:port, got " + quoted(s));
  return {s.substr(0, colon), parse_port(s.substr(colon + 1))};
}

std::vector<std::pair<std::string, int>> parse_shards(const std::string& csv) {
  std::vector<std::pair<std::string, int>> shards;
  std::istringstream in(csv);
  std::string item;
  while (std::getline(in, item, ',')) shards.push_back(parse_host_port(item));
  return shards;
}

Mode parse_mode(const std::string& s) {
  for (Mode mode : {Mode::Local, Mode::Redis, Mode::Hybrid, Mode::Naive})
    if (s == mode_name(mode)) return mode;
  fail("mode must be local, redis, hybrid or naive, got " + quoted(s));
}

bool parse_algo(const std::string& s, Algo& out) {
  for (Algo algo : {Algo::FixedWindow, Algo::SlidingLog, Algo::SlidingCounter, Algo::TokenBucket}) {
    if (s == algo_name(algo)) {
      out = algo;
      return true;
    }
  }
  return false;
}

bool parse_fail_mode(const std::string& s, FailMode& out) {
  if (s == "open") out = FailMode::Open;
  else if (s == "closed") out = FailMode::Closed;
  else if (s == "local") out = FailMode::Local;
  else return false;
  return true;
}

bool is_valid_method(const std::string& m) {
  if (m == "*") return true;
  if (m.empty()) return false;
  for (char c : m)
    if (c < 'A' || c > 'Z') return false;
  return true;
}

bool is_valid_key_by(const std::string& k) {
  const std::string kHeaderPrefix = "header:";
  if (k == "ip") return true;
  return k.size() > kHeaderPrefix.size() && k.compare(0, kHeaderPrefix.size(), kHeaderPrefix) == 0;
}

std::vector<std::string> split_whitespace(const std::string& line) {
  std::vector<std::string> fields;
  std::istringstream in(line);
  std::string field;
  while (in >> field) fields.push_back(field);
  return fields;
}

// name method path_prefix key_by algo limit window_ms fail
Rule parse_rule_line(const std::string& line) {
  constexpr size_t kColumns = 8;
  std::vector<std::string> f = split_whitespace(line);
  if (f.size() != kColumns) fail("expected 8 columns, got " + std::to_string(f.size()));

  Rule rule;
  rule.name = f[0];
  rule.method = f[1];
  rule.path_prefix = f[2];
  rule.key_by = f[3];
  if (!is_valid_method(rule.method)) fail("bad method " + quoted(rule.method));
  if (rule.path_prefix.empty() || rule.path_prefix[0] != '/')
    fail("path prefix must start with '/', got " + quoted(rule.path_prefix));
  if (!is_valid_key_by(rule.key_by)) fail("key_by must be 'ip' or 'header:<Name>', got " + quoted(rule.key_by));
  if (!parse_algo(f[4], rule.algo)) fail("unknown algo " + quoted(f[4]));
  rule.limit = positive_or_fail(f[5], "limit");
  rule.window_ms = positive_or_fail(f[6], "window_ms");
  if (!parse_fail_mode(f[7], rule.fail)) fail("fail must be open, closed or local, got " + quoted(f[7]));
  return rule;
}

std::string strip_comment(const std::string& line) { return line.substr(0, line.find('#')); }

bool is_blank(const std::string& s) { return s.find_first_not_of(" \t\r") == std::string::npos; }

std::string_view path_of(std::string_view target) { return target.substr(0, target.find('?')); }

// Whole-segment prefix match: /api matches /api and /api/x, but not /apix.
bool path_matches(std::string_view path, std::string_view prefix) {
  if (path.compare(0, prefix.size(), prefix) != 0) return false;
  bool exact = path.size() == prefix.size();
  bool prefix_ends_with_slash = prefix.back() == '/';
  bool next_char_is_slash = path.size() > prefix.size() && path[prefix.size()] == '/';
  return exact || prefix_ends_with_slash || next_char_is_slash;
}

void validate(const Options& o) {
  bool needs_redis = !o.serve_upstream && o.mode != Mode::Local;
  if (needs_redis && o.redis.empty()) fail("this --mode needs --redis host:port[,host:port...]");
}

}  // namespace

std::vector<Rule> parse_rules(std::istream& in) {
  std::vector<Rule> rules;
  std::unordered_set<std::string> names;
  std::string line;
  int line_number = 0;
  while (std::getline(in, line)) {
    line_number++;
    std::string content = strip_comment(line);
    if (is_blank(content)) continue;
    try {
      Rule rule = parse_rule_line(content);
      if (!names.insert(rule.name).second) fail("duplicate rule name " + quoted(rule.name));
      rules.push_back(std::move(rule));
    } catch (const std::runtime_error& e) {
      fail("line " + std::to_string(line_number) + ": " + e.what());
    }
  }
  return rules;
}

Options parse_args(int argc, const char* const* argv) {
  static const std::set<std::string> kFlagsWithValue = {
      "--port", "--upstream", "--rules", "--mode", "--redis", "--nodes", "--redis-timeout-ms", "--lua-dir"};
  Options o;
  for (int i = 1; i < argc; i++) {
    const std::string flag = argv[i];
    if (flag == "--serve-upstream") {
      o.serve_upstream = true;
      continue;
    }
    if (!kFlagsWithValue.count(flag)) fail("unknown flag " + flag);
    if (i + 1 >= argc) fail("missing value for " + flag);
    const std::string value = argv[++i];

    if (flag == "--port") {
      o.port = parse_port(value);
    } else if (flag == "--upstream") {
      std::tie(o.upstream_host, o.upstream_port) = parse_host_port(value);
    } else if (flag == "--rules") {
      o.rules_path = value;
    } else if (flag == "--mode") {
      o.mode = parse_mode(value);
    } else if (flag == "--redis") {
      o.redis = parse_shards(value);
    } else if (flag == "--nodes") {
      o.nodes = static_cast<int>(positive_or_fail(value, flag));
    } else if (flag == "--redis-timeout-ms") {
      o.redis_timeout_ms = positive_or_fail(value, flag);
    } else if (flag == "--lua-dir") {
      o.lua_dir = value;
    }
  }
  validate(o);
  return o;
}

const Rule* match_rule(const std::vector<Rule>& rules, std::string_view method, std::string_view target) {
  std::string_view path = path_of(target);
  for (const Rule& rule : rules) {
    bool method_matches = rule.method == "*" || rule.method == method;
    if (method_matches && path_matches(path, rule.path_prefix)) return &rule;
  }
  return nullptr;
}

std::unique_ptr<Limiter> make_limiter(Algo algo, int64_t limit, int64_t window_ms) {
  switch (algo) {
    case Algo::FixedWindow: return std::make_unique<FixedWindowLimiter>(limit, window_ms);
    case Algo::SlidingLog: return std::make_unique<SlidingLogLimiter>(limit, window_ms);
    case Algo::SlidingCounter: return std::make_unique<SlidingCounterLimiter>(limit, window_ms);
    case Algo::TokenBucket: return std::make_unique<TokenBucketLimiter>(limit, window_ms);
  }
  return nullptr;
}

const char* mode_name(Mode mode) {
  switch (mode) {
    case Mode::Local: return "local";
    case Mode::Redis: return "redis";
    case Mode::Hybrid: return "hybrid";
    case Mode::Naive: return "naive";
  }
  return "unknown";
}

const char* algo_name(Algo algo) {
  switch (algo) {
    case Algo::FixedWindow: return "fixed_window";
    case Algo::SlidingLog: return "sliding_log";
    case Algo::SlidingCounter: return "sliding_counter";
    case Algo::TokenBucket: return "token_bucket";
  }
  return "unknown";
}
