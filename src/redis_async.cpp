#include "redis_async.h"

#include <hiredis/async.h>
#include <hiredis/hiredis.h>
#include <sys/epoll.h>
#include <sys/time.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>

namespace {

constexpr int64_t kReconnectDelayMs = 500;
constexpr int64_t kMsPerSecond = 1000;
constexpr int64_t kUsPerMs = 1000;

timeval to_timeval(int64_t ms) {
  timeval tv{};
  tv.tv_sec = ms / kMsPerSecond;
  tv.tv_usec = (ms % kMsPerSecond) * kUsPerMs;
  return tv;
}

int64_t to_ms(const timeval& tv) { return tv.tv_sec * kMsPerSecond + tv.tv_usec / kUsPerMs; }

RedisResult failure(std::string why) {
  RedisResult result;
  result.error = std::move(why);
  return result;
}

bool parse_integer(const char* text, size_t len, long long& out) {
  if (len == 0) return false;
  std::string digits(text, len);
  char* end = nullptr;
  errno = 0;
  long long value = std::strtoll(digits.c_str(), &end, 10);
  if (errno != 0 || end != digits.c_str() + digits.size()) return false;
  out = value;
  return true;
}

void collect_values(const redisReply* reply, RedisResult& result) {
  switch (reply->type) {
    case REDIS_REPLY_INTEGER:
      result.values.push_back(reply->integer);
      break;
    case REDIS_REPLY_STRING: {
      long long value = 0;
      if (parse_integer(reply->str, reply->len, value)) result.values.push_back(value);
      if (result.text.empty()) result.text.assign(reply->str, reply->len);
      break;
    }
    case REDIS_REPLY_STATUS:
      result.text.assign(reply->str, reply->len);
      break;
    case REDIS_REPLY_ARRAY:
      for (size_t i = 0; i < reply->elements; i++) collect_values(reply->element[i], result);
      break;
    default:  // nil and friends carry no value
      break;
  }
}

// A NULL reply means the command never got an answer: timeout, disconnect, or shutdown.
RedisResult to_result(const redisReply* reply, const redisAsyncContext* ctx) {
  if (!reply) return failure(ctx->err ? ctx->errstr : "connection closed");
  if (reply->type == REDIS_REPLY_ERROR) return failure(std::string(reply->str, reply->len));
  RedisResult result;
  result.ok = true;
  collect_values(reply, result);
  return result;
}

bool starts_with(const std::string& s, const char* prefix) { return s.rfind(prefix, 0) == 0; }

// EVAL/EVALSHA <script-or-sha> 1 <key> <args...>
std::vector<std::string> script_argv(const char* verb, const std::string& script_or_sha, const std::string& key,
                                     const std::vector<long long>& args) {
  std::vector<std::string> argv = {verb, script_or_sha, "1", key};
  for (long long arg : args) argv.push_back(std::to_string(arg));
  return argv;
}

RedisShard* shard_of(void* data) { return static_cast<RedisShard*>(data); }

}  // namespace

LuaScripts load_lua_scripts(const std::string& dir, const std::vector<std::string>& names) {
  LuaScripts scripts;
  for (const std::string& name : names) {
    const std::string path = dir + "/" + name + ".lua";
    std::ifstream in(path);
    if (!in) throw std::runtime_error("cannot read " + path);
    std::stringstream source;
    source << in.rdbuf();
    scripts[name] = source.str();
  }
  return scripts;
}

RedisShard::RedisShard(EventLoop& loop, std::string host, int port, int64_t timeout_ms, const LuaScripts& scripts)
    : loop_(loop),
      host_(std::move(host)),
      port_(port),
      timeout_ms_(timeout_ms),
      scripts_(scripts),
      name_(host_ + ":" + std::to_string(port)) {}

RedisShard::~RedisShard() {
  destroying_ = true;  // from here on, replies are dropped instead of calling back
  if (reconnect_timer_) loop_.cancel_timer(reconnect_timer_);
  if (ctx_) redisAsyncFree(ctx_);  // runs pending callbacks with NULL, then on_context_freed
}

void RedisShard::connect() {
  if (ctx_ || destroying_) return;
  reconnect_timer_ = 0;

  redisOptions options{};
  REDIS_OPTIONS_SET_TCP(&options, host_.c_str(), port_);
  timeval timeout = to_timeval(timeout_ms_);  // hiredis copies it
  options.connect_timeout = &timeout;
  options.command_timeout = &timeout;

  ctx_ = redisAsyncConnectWithOptions(&options);
  if (!ctx_ || ctx_->err) {
    if (ctx_) redisAsyncFree(ctx_);  // not attached yet, so no cleanup hook runs
    ctx_ = nullptr;
    schedule_reconnect();
    return;
  }
  ctx_->data = this;
  attach_to_loop();  // must come before the connect callback, which asks for write events
  redisAsyncSetConnectCallback(ctx_, &RedisShard::on_connect);
}

void RedisShard::schedule_reconnect() {
  if (destroying_ || reconnect_timer_) return;
  reconnect_timer_ = loop_.add_timer(kReconnectDelayMs, [this] {
    reconnect_timer_ = 0;
    connect();
  });
}

void RedisShard::eval(const std::string& script, const std::string& key, const std::vector<long long>& args,
                      RedisCb cb) {
  if (!scripts_.count(script)) {
    cb(failure("unknown script " + script));
    return;
  }
  auto sha = shas_.find(script);
  if (sha == shas_.end()) {
    eval_source(script, key, args, std::move(cb));
    return;
  }
  send(script_argv("EVALSHA", sha->second, key, args),
       [this, script, key, args, cb = std::move(cb)](RedisResult result) mutable {
         // Redis restarted or ran SCRIPT FLUSH: send the source once, which re-caches it.
         bool cache_miss = !result.ok && starts_with(result.error, "NOSCRIPT");
         if (cache_miss) {
           eval_source(script, key, args, std::move(cb));
           return;
         }
         cb(std::move(result));
       });
}

void RedisShard::eval_source(const std::string& script, const std::string& key, const std::vector<long long>& args,
                             RedisCb cb) {
  send(script_argv("EVAL", scripts_.at(script), key, args), std::move(cb));
}

void RedisShard::command(const std::vector<std::string>& argv, RedisCb cb) { send(argv, std::move(cb)); }

void RedisShard::send(const std::vector<std::string>& argv, RedisCb cb) {
  if (!ctx_) {
    cb(failure("not connected"));  // fail fast; the caller decides what a missing answer means
    return;
  }
  std::vector<const char*> parts;
  std::vector<size_t> lengths;
  for (const std::string& part : argv) {
    parts.push_back(part.data());
    lengths.push_back(part.size());
  }
  auto pending = std::make_unique<Pending>(Pending{this, std::move(cb)});
  int rc = redisAsyncCommandArgv(ctx_, &RedisShard::on_reply, pending.get(), static_cast<int>(argv.size()),
                                 parts.data(), lengths.data());
  if (rc != REDIS_OK) {
    pending->cb(failure("command not accepted (connection closing)"));
    return;
  }
  pending.release();  // hiredis hands it back to on_reply exactly once
}

void RedisShard::cache_script_shas() {
  for (const auto& [name, source] : scripts_) {
    send({"SCRIPT", "LOAD", source}, [this, name = name](RedisResult result) {
      if (result.ok && !result.text.empty()) shas_[name] = result.text;
    });
  }
}

// ---- event-loop adapter -------------------------------------------------

void RedisShard::attach_to_loop() {
  fd_ = ctx_->c.fd;
  want_read_ = false;
  want_write_ = false;
  loop_.add_fd(fd_, 0, [this](uint32_t events) { on_socket_event(events); });

  ctx_->ev.data = this;
  ctx_->ev.addRead = [](void* data) { shard_of(data)->set_interest(true, shard_of(data)->want_write_); };
  ctx_->ev.delRead = [](void* data) { shard_of(data)->set_interest(false, shard_of(data)->want_write_); };
  ctx_->ev.addWrite = [](void* data) { shard_of(data)->set_interest(shard_of(data)->want_read_, true); };
  ctx_->ev.delWrite = [](void* data) { shard_of(data)->set_interest(shard_of(data)->want_read_, false); };
  ctx_->ev.cleanup = [](void* data) { shard_of(data)->on_context_freed(); };
  ctx_->ev.scheduleTimer = [](void* data, timeval tv) { shard_of(data)->on_timer_requested(tv); };
}

void RedisShard::set_interest(bool read, bool write) {
  want_read_ = read;
  want_write_ = write;
  uint32_t events = 0;
  if (read) events |= EPOLLIN;
  if (write) events |= EPOLLOUT;
  loop_.mod_fd(fd_, events);
}

void RedisShard::on_socket_event(uint32_t events) {
  // Handling a read can free the context (EOF, error), so re-check before writing.
  if (ctx_ && (events & (EPOLLIN | EPOLLERR | EPOLLHUP))) redisAsyncHandleRead(ctx_);
  if (ctx_ && (events & EPOLLOUT)) redisAsyncHandleWrite(ctx_);
}

// hiredis re-arms this every time it sends; it's both the connect and the command timeout.
void RedisShard::on_timer_requested(const timeval& tv) {
  if (timeout_timer_) loop_.cancel_timer(timeout_timer_);
  int64_t delay_ms = std::max<int64_t>(1, to_ms(tv));
  timeout_timer_ = loop_.add_timer(delay_ms, [this] {
    timeout_timer_ = 0;
    if (ctx_) redisAsyncHandleTimeout(ctx_);  // no-op if nothing is outstanding
  });
}

// The single place a dead connection is handled, whatever killed it.
void RedisShard::on_context_freed() {
  loop_.del_fd(fd_);  // hiredis closes the fd right after this returns
  fd_ = -1;
  if (timeout_timer_) {
    loop_.cancel_timer(timeout_timer_);
    timeout_timer_ = 0;
  }
  ctx_ = nullptr;
  connected_ = false;
  schedule_reconnect();
}

// ---- hiredis callbacks --------------------------------------------------

void RedisShard::on_reply(redisAsyncContext* ctx, void* reply, void* privdata) {
  std::unique_ptr<Pending> pending(static_cast<Pending*>(privdata));
  if (pending->shard->destroying_) return;
  pending->cb(to_result(static_cast<const redisReply*>(reply), ctx));
}

void RedisShard::on_connect(const redisAsyncContext* ctx, int status) {
  RedisShard* shard = shard_of(ctx->data);
  if (status != REDIS_OK) return;  // hiredis frees the context next; on_context_freed retries
  shard->connected_ = true;
  shard->cache_script_shas();
}
