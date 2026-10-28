#pragma once
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <string_view>

uint64_t hash64(std::string_view s);  // FNV-1a 64-bit followed by a fmix64 finalizer

// Consistent hashing with virtual nodes. Spec §5.4.
class HashRing {
 public:
  explicit HashRing(int vnodes_per_node = 100);
  void add_node(const std::string& node);     // no-op if present
  void remove_node(const std::string& node);  // no-op if absent
  const std::string& node_for(std::string_view key) const;  // throws std::runtime_error if empty
  size_t size() const;                        // physical nodes

 private:
  int vnodes_;
  // TODO(Ali): your ring + node set
};
