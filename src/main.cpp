#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <csignal>
#include <fstream>
#include <iostream>

#include "config.h"
#include "decider.h"
#include "event_loop.h"
#include "metrics.h"
#include "server.h"
#include "socket.h"
#include "upstream_pool.h"

namespace {

constexpr int64_t kUpstreamTimeoutMs = 2'000;
constexpr int64_t kShutdownGraceMs = 2'000;
constexpr int64_t kShutdownPollMs = 50;

std::vector<Rule> load_rules(const std::string& path) {
  if (path.empty()) throw std::runtime_error("--rules <file> is required for a gateway");
  std::ifstream in(path);
  if (!in) throw std::runtime_error("cannot open rules file " + path);
  return parse_rules(in);
}

// SIGINT/SIGTERM arrive as readable events on a signalfd instead of interrupting
// arbitrary code, so shutdown runs as an ordinary event-loop callback.
Fd watch_shutdown_signals() {
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  sigprocmask(SIG_BLOCK, &signals, nullptr);
  Fd fd(signalfd(-1, &signals, SFD_NONBLOCK | SFD_CLOEXEC));
  if (!fd) throw std::runtime_error("signalfd failed");
  return fd;
}

// Stop accepting, let in-flight requests finish (bounded by a grace period), then stop.
void shut_down_gracefully(EventLoop& loop, Server& server, int64_t deadline_ms) {
  server.stop_accepting();
  if (!server.has_requests_in_flight() || EventLoop::now_ms() >= deadline_ms) {
    loop.stop();
    return;
  }
  loop.add_timer(kShutdownPollMs, [&loop, &server, deadline_ms] { shut_down_gracefully(loop, server, deadline_ms); });
}

void run_until_signalled(EventLoop& loop, Server& server) {
  Fd signals = watch_shutdown_signals();
  loop.add_fd(signals.get(), EPOLLIN, [&loop, &server, &signals](uint32_t) {
    signalfd_siginfo info;
    while (read(signals.get(), &info, sizeof info) == sizeof info) {
    }
    loop.del_fd(signals.get());
    shut_down_gracefully(loop, server, EventLoop::now_ms() + kShutdownGraceMs);
  });
  loop.run();
}

int serve_upstream(const Options& options) {
  EventLoop loop;
  Metrics metrics;
  Server server(loop, options, {}, metrics, nullptr, nullptr);
  server.start();
  std::cerr << "upstream listening on :" << options.port << "\n";
  run_until_signalled(loop, server);
  return 0;
}

int serve_gateway(const Options& options) {
  EventLoop loop;
  Metrics metrics;
  std::vector<Rule> rules = load_rules(options.rules_path);
  size_t rule_count = rules.size();
  UpstreamPool upstream(loop, options.upstream_host, options.upstream_port, kUpstreamTimeoutMs);
  Decider decider(loop, options, metrics);
  Server server(loop, options, std::move(rules), metrics, &decider, &upstream);
  server.start();
  std::cerr << "gateway listening on :" << options.port << " (mode " << mode_name(options.mode) << ", "
            << rule_count << " rules, upstream " << options.upstream_host << ":" << options.upstream_port
            << ")\n";
  run_until_signalled(loop, server);
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);  // hiredis writes with write(); a closed peer must not kill us
  try {
    Options options = parse_args(argc, argv);
    return options.serve_upstream ? serve_upstream(options) : serve_gateway(options);
  } catch (const std::exception& e) {
    std::cerr << "gateway: " << e.what() << "\n";
    return 2;
  }
}
