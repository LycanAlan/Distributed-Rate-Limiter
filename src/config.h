#pragma once
#include <cstdint>
#include <istream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "limiters.h"
#include "types.h"

enum class Mode { Local, Redis, Hybrid, Naive };
enum class FailMode { Open, Closed, Local };

// One line of rules.conf (spec §6).
struct Rule {
  std::string name;
  std::string method;       // "GET", "POST", ... or "*"
  std::string path_prefix;  // matches whole path segments: /api matches /api and /api/x, not /apix
  std::string key_by;       // "ip" or "header:<Name>"
  Algo algo;
  int64_t limit, window_ms;
  FailMode fail;
};

struct Options {
  int port = 8080;
  bool serve_upstream = false;
  std::string upstream_host = "127.0.0.1";
  int upstream_port = 9000;
  std::string rules_path;
  Mode mode = Mode::Local;
  std::vector<std::pair<std::string, int>> redis;  // shards
  int nodes = 1;
  int64_t redis_timeout_ms = 50;
  std::string lua_dir = LUA_DIR;  // compiled-in path to the repo's lua/ folder
};

std::vector<Rule> parse_rules(std::istream& in);         // throws std::runtime_error("line N: ...")
Options parse_args(int argc, const char* const* argv);  // throws std::runtime_error
const Rule* match_rule(const std::vector<Rule>& rules, std::string_view method, std::string_view target);

std::unique_ptr<Limiter> make_limiter(Algo algo, int64_t limit, int64_t window_ms);
const char* algo_name(Algo algo);  // same as the lua/<name>.lua script
const char* mode_name(Mode mode);  // as spelled on the command line
