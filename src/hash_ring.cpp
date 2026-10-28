#include "hash_ring.h"

#include <stdexcept>

// TODO(Ali): implement. Tests: tests/test_hash_ring.cpp
// Hints: docs/plans/2026-10-26-rate-limiter-plan.md, Task U3.

uint64_t hash64(std::string_view) { return 0; }

HashRing::HashRing(int vnodes_per_node) : vnodes_(vnodes_per_node) {}

void HashRing::add_node(const std::string&) {}

void HashRing::remove_node(const std::string&) {}

const std::string& HashRing::node_for(std::string_view) const {
  throw std::runtime_error("TODO(Ali): HashRing::node_for");
}

size_t HashRing::size() const { return 0; }
