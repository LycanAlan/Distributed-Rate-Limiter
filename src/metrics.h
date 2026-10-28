#pragma once
#include <array>
#include <cstdint>
#include <map>
#include <string>

// Counters, gauges and one latency histogram, rendered in the Prometheus text
// format for GET /metrics. A series is a name plus labels, written out in full:
//   rl_requests_total{decision="allowed"}
class Metrics {
 public:
  void inc(const std::string& series, int64_t by = 1);  // counter
  void set(const std::string& series, int64_t value);   // gauge
  void observe_latency_us(int64_t us);                  // rl_decision_latency_us
  int64_t get(const std::string& series) const;         // counter or gauge, 0 if unset
  std::string render() const;

 private:
  static constexpr std::array<int64_t, 9> kBucketsUs = {50, 100, 250, 500, 1000, 2500, 5000, 10000, 50000};
  std::map<std::string, int64_t> counters_, gauges_;
  std::array<int64_t, kBucketsUs.size() + 1> buckets_{};  // last one is +Inf
  int64_t sum_us_ = 0, count_ = 0;
};
