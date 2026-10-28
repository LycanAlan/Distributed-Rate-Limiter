#include "decider.h"

#include <stdexcept>

Decider::Decider(EventLoop& loop, const Options& options, Metrics& metrics)
    : loop_(loop), options_(options), metrics_(metrics) {
  if (options_.mode != Mode::Local)
    throw std::runtime_error(std::string("--mode ") + mode_name(options_.mode) +
                             " is not built yet (plan tasks C8, C9)");
}

void Decider::decide(const Rule& rule, const std::string& client_key, DecideCb on_decided) {
  on_decided(local_limiter(rule).allow(client_key, EventLoop::now_ms()));
}

Limiter& Decider::local_limiter(const Rule& rule) {
  std::unique_ptr<Limiter>& limiter = local_limiters_[rule.name];
  if (!limiter) limiter = make_limiter(rule.algo, rule.limit, rule.window_ms);
  return *limiter;
}
