#pragma once
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "config.h"
#include "decider.h"
#include "event_loop.h"
#include "http.h"
#include "metrics.h"
#include "socket.h"
#include "upstream_pool.h"

// Accepts HTTP clients and serves them one request at a time per connection.
// As a gateway it rate-limits and proxies; with no decider/upstream it acts as
// the dummy upstream and answers every request with 200 "ok".
//
// Clients are tracked by id, never by pointer: an async reply (decider or
// upstream) for a client that has since disconnected finds no entry and is dropped.
class Server {
 public:
  Server(EventLoop& loop, const Options& options, std::vector<Rule> rules, Metrics& metrics,
         Decider* decider, UpstreamPool* upstream);
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  void start();
  void stop_accepting();
  bool has_requests_in_flight() const;

 private:
  struct Client {
    Fd fd;
    std::string ip;
    std::string in, out;
    bool busy = false;  // a request is being decided or proxied
    bool close_after_write = false;
    uint64_t idle_timer = 0;
  };

  void accept_pending();
  void on_client_event(uint64_t id, uint32_t events);
  void process_input(uint64_t id);
  void handle_request(uint64_t id, HttpRequest request);
  void decide_then_forward(uint64_t id, const Rule& rule, HttpRequest request);
  void forward(uint64_t id, HttpRequest request, std::vector<Header> extra_response_headers);
  void respond(uint64_t id, HttpResponse response);
  void flush(uint64_t id);
  void arm_idle_timer(uint64_t id);
  void close_client(uint64_t id);
  Client* find(uint64_t id);

  EventLoop& loop_;
  const Options& options_;
  const std::vector<Rule> rules_;  // never modified, so Rule pointers into it stay valid
  Metrics& metrics_;
  Decider* decider_;
  UpstreamPool* upstream_;
  bool is_upstream_ = false;
  Fd listen_fd_;
  uint64_t next_client_id_ = 1;
  std::unordered_map<uint64_t, Client> clients_;
};
