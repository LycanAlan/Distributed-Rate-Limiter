#include "upstream_pool.h"

#include <sys/epoll.h>

#include <algorithm>

namespace {
constexpr size_t kMaxIdleConnections = 64;
}

UpstreamPool::UpstreamPool(EventLoop& loop, const std::string& host, int port, int64_t timeout_ms)
    : loop_(loop), addr_(resolve_ipv4(host, port)), timeout_ms_(timeout_ms) {}

UpstreamPool::~UpstreamPool() {
  for (auto& [id, conn] : conns_) {
    loop_.del_fd(conn.fd.get());
    if (conn.timeout_timer) loop_.cancel_timer(conn.timeout_timer);
  }
}

void UpstreamPool::send(std::string request_bytes, ResponseCb on_response) {
  uint64_t id = 0;
  if (!idle_.empty()) {
    id = idle_.back();
    idle_.pop_back();
  } else {
    id = open_connection();
  }
  if (id == 0) {
    on_response(std::nullopt);
    return;
  }

  Conn& conn = conns_.at(id);
  conn.out = std::move(request_bytes);
  conn.in.clear();
  conn.on_response = std::move(on_response);
  conn.timeout_timer = loop_.add_timer(timeout_ms_, [this, id] {
    auto it = conns_.find(id);
    if (it == conns_.end()) return;
    it->second.timeout_timer = 0;
    finish(id, std::nullopt, /*reusable=*/false);
  });
  loop_.mod_fd(conn.fd.get(), EPOLLIN | EPOLLOUT);
}

uint64_t UpstreamPool::open_connection() {
  Fd fd = connect_tcp(addr_);
  if (!fd) return 0;
  uint64_t id = next_id_++;
  int raw_fd = fd.get();
  Conn& conn = conns_[id];
  conn.fd = std::move(fd);
  conn.connecting = true;
  loop_.add_fd(raw_fd, EPOLLOUT, [this, id](uint32_t events) { on_event(id, events); });
  return id;
}

void UpstreamPool::on_event(uint64_t id, uint32_t events) {
  auto it = conns_.find(id);
  if (it == conns_.end()) return;
  Conn& conn = it->second;

  bool idle = !conn.on_response;
  if (idle) {  // an idle connection only becomes readable when the upstream closes it
    drop(id);
    return;
  }
  if (events & EPOLLERR) {
    finish(id, std::nullopt, /*reusable=*/false);
    return;
  }
  if (events & EPOLLOUT) {
    on_writable(id, conn);
    if (!conns_.count(id)) return;
  }
  if (events & (EPOLLIN | EPOLLHUP)) on_readable(id, conn);
}

void UpstreamPool::on_writable(uint64_t id, Conn& conn) {
  if (conn.connecting) {
    if (!connect_succeeded(conn.fd.get())) {
      finish(id, std::nullopt, /*reusable=*/false);
      return;
    }
    conn.connecting = false;
  }
  IoResult result = write_available(conn.fd.get(), conn.out);
  if (result == IoResult::Error) {
    finish(id, std::nullopt, /*reusable=*/false);
    return;
  }
  if (result == IoResult::Ok) loop_.mod_fd(conn.fd.get(), EPOLLIN);  // all sent: stop asking for EPOLLOUT
}

void UpstreamPool::on_readable(uint64_t id, Conn& conn) {
  IoResult read_result = read_available(conn.fd.get(), conn.in);
  HttpResponse response;
  ParseStatus parsed = parse_response(conn.in, response);
  if (parsed == ParseStatus::Done) {
    bool reusable = response.keep_alive && conn.in.empty() && read_result == IoResult::Ok;
    finish(id, std::move(response), reusable);
    return;
  }
  bool stream_ended = read_result != IoResult::Ok;
  if (parsed != ParseStatus::Incomplete || stream_ended) finish(id, std::nullopt, /*reusable=*/false);
}

// ponytail: a pooled connection the upstream closed a moment ago can fail one
// request with 502; retry once on a fresh connection if that shows up in tests.
void UpstreamPool::finish(uint64_t id, std::optional<HttpResponse> response, bool reusable) {
  Conn& conn = conns_.at(id);
  if (conn.timeout_timer) {
    loop_.cancel_timer(conn.timeout_timer);
    conn.timeout_timer = 0;
  }
  ResponseCb on_response = std::move(conn.on_response);
  conn.on_response = nullptr;

  // Back to the pool before the callback runs: the callback may send() again.
  if (reusable && idle_.size() < kMaxIdleConnections) {
    loop_.mod_fd(conn.fd.get(), EPOLLIN);
    idle_.push_back(id);
  } else {
    drop(id);
  }
  on_response(std::move(response));
}

void UpstreamPool::drop(uint64_t id) {
  auto it = conns_.find(id);
  if (it == conns_.end()) return;
  loop_.del_fd(it->second.fd.get());
  if (it->second.timeout_timer) loop_.cancel_timer(it->second.timeout_timer);
  conns_.erase(it);
  idle_.erase(std::remove(idle_.begin(), idle_.end(), id), idle_.end());
}
