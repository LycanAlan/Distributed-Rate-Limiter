#include "metrics.h"

namespace {

// rl_requests_total{decision="allowed"} -> rl_requests_total
std::string family_of(const std::string& series) { return series.substr(0, series.find('{')); }

// Prometheus wants one "# TYPE" line per family, followed by all of its series.
void render_group(std::string& out, const std::map<std::string, int64_t>& series, const char* type) {
  std::map<std::string, std::string> lines_by_family;
  for (const auto& [name, value] : series) lines_by_family[family_of(name)] += name + " " + std::to_string(value) + "\n";
  for (const auto& [family, lines] : lines_by_family) out += "# TYPE " + family + " " + type + "\n" + lines;
}

std::string bucket_line(const std::string& le, int64_t cumulative_count) {
  return "rl_decision_latency_us_bucket{le=\"" + le + "\"} " + std::to_string(cumulative_count) + "\n";
}

}  // namespace

void Metrics::inc(const std::string& series, int64_t by) { counters_[series] += by; }

void Metrics::set(const std::string& series, int64_t value) { gauges_[series] = value; }

void Metrics::observe_latency_us(int64_t us) {
  size_t bucket = 0;
  while (bucket < kBucketsUs.size() && us > kBucketsUs[bucket]) bucket++;
  buckets_[bucket]++;
  sum_us_ += us;
  count_++;
}

int64_t Metrics::get(const std::string& series) const {
  if (auto it = counters_.find(series); it != counters_.end()) return it->second;
  if (auto it = gauges_.find(series); it != gauges_.end()) return it->second;
  return 0;
}

std::string Metrics::render() const {
  std::string out;
  render_group(out, counters_, "counter");
  render_group(out, gauges_, "gauge");

  out += "# TYPE rl_decision_latency_us histogram\n";
  int64_t cumulative = 0;  // Prometheus buckets count everything <= le, so they accumulate
  for (size_t i = 0; i < kBucketsUs.size(); i++) {
    cumulative += buckets_[i];
    out += bucket_line(std::to_string(kBucketsUs[i]), cumulative);
  }
  cumulative += buckets_.back();
  out += bucket_line("+Inf", cumulative);
  out += "rl_decision_latency_us_sum " + std::to_string(sum_us_) + "\n";
  out += "rl_decision_latency_us_count " + std::to_string(count_) + "\n";
  return out;
}
