#pragma once
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "event_loop.h"

struct redisAsyncContext;
struct timeval;

struct RedisResult {
  bool ok = false;
  std::string error;             // when !ok: Redis error text, "Timeout", "not connected", ...
  std::vector<long long> values;  // integer replies, numeric strings, and arrays of them
  std::string text;               // status and string replies, e.g. "OK" or a script SHA
};
using RedisCb = std::function<void(RedisResult)>;
using LuaScripts = std::map<std::string, std::string>;  // script name -> Lua source

// Reads <dir>/<name>.lua for each name. Startup only (blocking file I/O). Throws.
LuaScripts load_lua_scripts(const std::string& dir, const std::vector<std::string>& names);

// One async connection to one Redis instance, driven by our EventLoop through a
// small custom hiredis adapter. While the connection is down it reconnects every
// 500 ms and commands fail fast instead of queueing.
//
// Callbacks run exactly once (reply, error, timeout or disconnect), except after
// the shard is destroyed: then they are dropped, so they never touch dead objects.
class RedisShard {
 public:
  RedisShard(EventLoop& loop, std::string host, int port, int64_t timeout_ms, const LuaScripts& scripts);
  ~RedisShard();
  RedisShard(const RedisShard&) = delete;
  RedisShard& operator=(const RedisShard&) = delete;

  void connect();
  bool connected() const { return connected_; }
  const std::string& name() const { return name_; }  // "host:port"

  // EVALSHA <sha> 1 <key> <args...>; falls back to EVAL with the source on NOSCRIPT.
  void eval(const std::string& script, const std::string& key, const std::vector<long long>& args, RedisCb cb);
  void command(const std::vector<std::string>& argv, RedisCb cb);

 private:
  struct Pending {
    RedisShard* shard;
    RedisCb cb;
  };

  void send(const std::vector<std::string>& argv, RedisCb cb);
  void eval_source(const std::string& script, const std::string& key, const std::vector<long long>& args, RedisCb cb);
  void cache_script_shas();
  void schedule_reconnect();

  // Event-loop adapter: hiredis tells us which events it wants on its socket.
  void attach_to_loop();
  void set_interest(bool read, bool write);
  void on_socket_event(uint32_t events);
  void on_timer_requested(const timeval& tv);
  void on_context_freed();

  // C callbacks handed to hiredis. Disconnects need none: the cleanup hook
  // (on_context_freed) runs for every way a context can die.
  static void on_reply(redisAsyncContext* ctx, void* reply, void* privdata);
  static void on_connect(const redisAsyncContext* ctx, int status);

  EventLoop& loop_;
  const std::string host_;
  const int port_;
  const int64_t timeout_ms_;
  const LuaScripts& scripts_;
  const std::string name_;
  std::map<std::string, std::string> shas_;  // script name -> SHA1, filled after connecting

  redisAsyncContext* ctx_ = nullptr;  // null while disconnected
  int fd_ = -1;
  bool want_read_ = false;
  bool want_write_ = false;
  bool connected_ = false;
  bool destroying_ = false;
  uint64_t timeout_timer_ = 0;
  uint64_t reconnect_timer_ = 0;
};
