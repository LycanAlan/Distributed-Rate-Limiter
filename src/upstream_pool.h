#pragma once
#include <netinet/in.h>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "event_loop.h"
#include "http.h"
#include "socket.h"

// Keep-alive connections to one upstream, reused across requests. One request
// in flight per connection. Pooling saves a TCP handshake per request and
// avoids piling up TIME_WAIT sockets.
class UpstreamPool {
 public:
  using ResponseCb = std::function<void(std::optional<HttpResponse>)>;  // nullopt: upstream failed

  UpstreamPool(EventLoop& loop, const std::string& host, int port, int64_t timeout_ms);
  ~UpstreamPool();
  UpstreamPool(const UpstreamPool&) = delete;
  UpstreamPool& operator=(const UpstreamPool&) = delete;

  // May call on_response synchronously if no connection can be opened.
  void send(std::string request_bytes, ResponseCb on_response);

 private:
  struct Conn {
    Fd fd;
    bool connecting = false;
    std::string out, in;
    ResponseCb on_response;  // empty while the connection sits idle in the pool
    uint64_t timeout_timer = 0;
  };

  uint64_t open_connection();  // 0 on failure
  void on_event(uint64_t id, uint32_t events);
  void on_writable(uint64_t id, Conn& conn);
  void on_readable(uint64_t id, Conn& conn);
  void finish(uint64_t id, std::optional<HttpResponse> response, bool reusable);
  void drop(uint64_t id);

  EventLoop& loop_;
  sockaddr_in addr_;
  int64_t timeout_ms_;
  uint64_t next_id_ = 1;
  std::unordered_map<uint64_t, Conn> conns_;
  std::vector<uint64_t> idle_;
};
