#include "server.h"

#include <sys/epoll.h>

#include <algorithm>
#include <chrono>

namespace {

constexpr int64_t kIdleTimeoutMs = 10'000;
constexpr size_t kMaxBufferedInput = kMaxHeaderBytes + kMaxBodyBytes;
const std::string kHeaderKeyPrefix = "header:";

const std::string kAllowed = "rl_requests_total{decision=\"allowed\"}";
const std::string kDenied = "rl_requests_total{decision=\"denied\"}";
const std::string kNoRule = "rl_requests_total{decision=\"no_rule\"}";
const std::string kUpstreamErrors = "rl_upstream_errors_total";

int64_t now_us() {
  using namespace std::chrono;
  return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

HttpResponse text_response(int status, std::string body, bool keep_alive) {
  HttpResponse response;
  response.status = status;
  response.headers = {{"Content-Type", "text/plain"}};
  response.body = std::move(body);
  response.keep_alive = keep_alive;
  return response;
}

std::vector<Header> rate_limit_headers(const Rule& rule, const Decision& decision) {
  return {{"X-RateLimit-Limit", std::to_string(rule.limit)},
          {"X-RateLimit-Remaining", std::to_string(decision.remaining)}};
}

HttpResponse rate_limited_response(const Rule& rule, const Decision& decision, bool keep_alive) {
  constexpr int64_t kMsPerSecond = 1000;
  // Retry-After is in whole seconds: round up, and never tell a client "retry in 0".
  int64_t retry_after_s = std::max<int64_t>(1, (decision.retry_after_ms + kMsPerSecond - 1) / kMsPerSecond);
  HttpResponse response = text_response(429, "rate limited\n", keep_alive);
  response.headers.push_back({"Retry-After", std::to_string(retry_after_s)});
  for (Header& h : rate_limit_headers(rule, decision)) response.headers.push_back(std::move(h));
  return response;
}

// The identity a rule counts against: the client IP, or a header such as X-User-Id.
// A request missing the header is counted by IP so it can't dodge the limit.
std::string client_key(const Rule& rule, const HttpRequest& request, const std::string& ip) {
  if (rule.key_by.compare(0, kHeaderKeyPrefix.size(), kHeaderKeyPrefix) != 0) return ip;
  const std::string* value = request.header(rule.key_by.substr(kHeaderKeyPrefix.size()));
  return value ? *value : ip;
}

}  // namespace

Server::Server(EventLoop& loop, const Options& options, std::vector<Rule> rules, Metrics& metrics,
               Decider* decider, UpstreamPool* upstream)
    : loop_(loop),
      options_(options),
      rules_(std::move(rules)),
      metrics_(metrics),
      decider_(decider),
      upstream_(upstream),
      is_upstream_(decider == nullptr) {}

Server::~Server() {
  for (auto& [id, client] : clients_) {
    loop_.del_fd(client.fd.get());
    if (client.idle_timer) loop_.cancel_timer(client.idle_timer);
  }
  stop_accepting();
}

void Server::start() {
  listen_fd_ = listen_tcp(options_.port);
  loop_.add_fd(listen_fd_.get(), EPOLLIN, [this](uint32_t) { accept_pending(); });
}

void Server::stop_accepting() {
  if (!listen_fd_) return;
  loop_.del_fd(listen_fd_.get());
  listen_fd_.reset();
}

bool Server::has_requests_in_flight() const {
  for (const auto& [id, client] : clients_)
    if (client.busy) return true;
  return false;
}

Server::Client* Server::find(uint64_t id) {
  auto it = clients_.find(id);
  return it == clients_.end() ? nullptr : &it->second;
}

void Server::accept_pending() {
  for (;;) {
    Fd fd = accept_client(listen_fd_.get());
    if (!fd) return;  // nothing pending (or out of fds: the listener stays readable, we retry)
    set_nodelay(fd.get());
    uint64_t id = next_client_id_++;
    int raw_fd = fd.get();
    Client& client = clients_[id];
    client.ip = peer_ip(raw_fd);
    client.fd = std::move(fd);
    loop_.add_fd(raw_fd, EPOLLIN, [this, id](uint32_t events) { on_client_event(id, events); });
    arm_idle_timer(id);
  }
}

void Server::on_client_event(uint64_t id, uint32_t events) {
  Client* client = find(id);
  if (!client) return;
  if (events & EPOLLERR) {
    close_client(id);
    return;
  }
  if (events & EPOLLOUT) {
    flush(id);
    client = find(id);
    if (!client) return;
  }
  if (events & (EPOLLIN | EPOLLHUP)) {
    IoResult result = read_available(client->fd.get(), client->in);
    bool gone = result == IoResult::Closed || result == IoResult::Error;
    if (gone || client->in.size() > kMaxBufferedInput) {
      close_client(id);  // any reply still in flight for this id gets dropped
      return;
    }
    process_input(id);
  }
}

void Server::process_input(uint64_t id) {
  Client* client = find(id);
  if (!client || client->busy) return;

  HttpRequest request;
  switch (parse_request(client->in, request)) {
    case ParseStatus::Incomplete:
      return;
    case ParseStatus::Error:
      respond(id, text_response(400, "bad request\n", /*keep_alive=*/false));
      return;
    case ParseStatus::TooLarge:
      respond(id, text_response(431, "headers too large\n", /*keep_alive=*/false));
      return;
    case ParseStatus::Done:
      client->busy = true;
      if (client->idle_timer) {
        loop_.cancel_timer(client->idle_timer);
        client->idle_timer = 0;
      }
      handle_request(id, std::move(request));
      return;
  }
}

void Server::handle_request(uint64_t id, HttpRequest request) {
  if (is_upstream_) {
    respond(id, text_response(200, "ok\n", request.keep_alive));
    return;
  }
  if (request.target == "/healthz") {
    respond(id, text_response(200, "ok\n", request.keep_alive));
    return;
  }
  if (request.target == "/metrics") {
    HttpResponse response = text_response(200, metrics_.render(), request.keep_alive);
    response.headers = {{"Content-Type", "text/plain; version=0.0.4"}};
    respond(id, std::move(response));
    return;
  }

  const Rule* rule = match_rule(rules_, request.method, request.target);
  if (!rule) {
    metrics_.inc(kNoRule);
    forward(id, std::move(request), {});
    return;
  }
  decide_then_forward(id, *rule, std::move(request));
}

void Server::decide_then_forward(uint64_t id, const Rule& rule, HttpRequest request) {
  std::string key = client_key(rule, request, find(id)->ip);
  int64_t started_us = now_us();
  const Rule* rule_ptr = &rule;  // safe to capture: rules_ is const for the server's lifetime

  decider_->decide(rule, key, [this, id, rule_ptr, started_us, request = std::move(request)](Decision decision) mutable {
    metrics_.observe_latency_us(now_us() - started_us);
    if (!find(id)) return;  // client left while we were deciding
    if (!decision.allowed) {
      metrics_.inc(kDenied);
      respond(id, rate_limited_response(*rule_ptr, decision, request.keep_alive));
      return;
    }
    metrics_.inc(kAllowed);
    forward(id, std::move(request), rate_limit_headers(*rule_ptr, decision));
  });
}

void Server::forward(uint64_t id, HttpRequest request, std::vector<Header> extra_response_headers) {
  request.headers.push_back({"X-Forwarded-For", find(id)->ip});
  bool keep_alive = request.keep_alive;

  upstream_->send(serialize(request), [this, id, keep_alive, extra = std::move(extra_response_headers)](
                                          std::optional<HttpResponse> response) {
    if (!find(id)) return;
    if (!response) {
      metrics_.inc(kUpstreamErrors);
      respond(id, text_response(502, "bad gateway\n", keep_alive));
      return;
    }
    response->keep_alive = keep_alive;  // the client's connection, not the upstream's
    for (const Header& h : extra) response->headers.push_back(h);
    respond(id, std::move(*response));
  });
}

void Server::respond(uint64_t id, HttpResponse response) {
  Client* client = find(id);
  if (!client) return;
  if (!response.keep_alive) client->close_after_write = true;
  client->out += serialize(response);
  client->busy = false;
  flush(id);
}

void Server::flush(uint64_t id) {
  Client* client = find(id);
  if (!client) return;

  IoResult result = write_available(client->fd.get(), client->out);
  if (result == IoResult::Error) {
    close_client(id);
    return;
  }
  if (result == IoResult::WouldBlock) {
    loop_.mod_fd(client->fd.get(), EPOLLOUT);  // stop reading until the client drains our output
    return;
  }
  if (client->close_after_write) {
    close_client(id);
    return;
  }
  loop_.mod_fd(client->fd.get(), EPOLLIN);
  arm_idle_timer(id);
  process_input(id);  // a pipelined request may already be buffered
}

// ponytail: cancelled timers stay in the loop's heap until their deadline
// (lazy deletion); fine at benchmark rates, a timer wheel if churn ever grows.
void Server::arm_idle_timer(uint64_t id) {
  Client* client = find(id);
  if (!client) return;
  if (client->idle_timer) loop_.cancel_timer(client->idle_timer);
  client->idle_timer = loop_.add_timer(kIdleTimeoutMs, [this, id] {
    Client* idle_client = find(id);
    if (!idle_client) return;
    idle_client->idle_timer = 0;
    if (!idle_client->busy) close_client(id);
  });
}

void Server::close_client(uint64_t id) {
  Client* client = find(id);
  if (!client) return;
  if (client->idle_timer) loop_.cancel_timer(client->idle_timer);
  loop_.del_fd(client->fd.get());  // before the Fd closes, so epoll never sees a dead fd
  clients_.erase(id);
}
