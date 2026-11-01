// Consistent hashing: which Redis shard owns a given rate-limit key?
//
// The obvious answer, hash(key) % N, reshuffles almost everything when N
// changes. Going from 3 to 4 shards moves about 75% of keys, and every key
// that moves starts again from a count of zero.
//
// Instead, picture the 2^64 possible hash values as a circle. Each shard is
// placed on it at hash(shard name). A key is hashed onto the same circle and
// belongs to the first shard found walking clockwise:
//
//   0 ──────●──────────────●────────●────────────────●───── 2^64, wraps to 0
//          a#7          key k     b#40              c#2
//                         └─ walk ─┘    k belongs to b
//
// A new shard takes over only the arc just before each of its points, so only
// about 1/N of keys move, and every one of them moves to the new shard.
//
// With one point per shard the arcs would be very uneven (one shard could own
// half the circle). So each shard is placed `vnodes` times, at hash("a#0"),
// hash("a#1"), ... These "virtual nodes" even out, and each shard ends up with
// close to 1/N of the keys.

#include "hash_ring.h"

#include <stdexcept>

namespace {

// FNV-1a: one xor and one multiply per byte. Fast, but similar inputs such as
// "a#1" and "a#2" get similar hashes, which would bunch points together.
uint64_t fnv1a(std::string_view s) {
  constexpr uint64_t kOffsetBasis = 14695981039346656037ULL;
  constexpr uint64_t kPrime = 1099511628211ULL;
  uint64_t hash = kOffsetBasis;
  for (unsigned char byte : s) {
    hash ^= byte;
    hash *= kPrime;
  }
  return hash;
}

// MurmurHash3's finalizer: spreads every input bit over the whole output, so a
// one-character change flips about half of the 64 bits.
uint64_t fmix64(uint64_t hash) {
  hash ^= hash >> 33;
  hash *= 0xff51afd7ed558ccdULL;
  hash ^= hash >> 33;
  hash *= 0xc4ceb9fe1a85ec53ULL;
  hash ^= hash >> 33;
  return hash;
}

std::string vnode_label(const std::string& node, int index) { return node + "#" + std::to_string(index); }

}  // namespace

uint64_t hash64(std::string_view s) { return fmix64(fnv1a(s)); }

HashRing::HashRing(int vnodes_per_node) : vnodes_(vnodes_per_node) {}

void HashRing::add_node(const std::string& node) {
  const bool already_present = !nodes_.insert(node).second;
  if (already_present) return;
  for (int i = 0; i < vnodes_; i++) {
    ring_.emplace(hash64(vnode_label(node, i)), node);  // on a 64-bit collision the first owner keeps the point
  }
}

void HashRing::remove_node(const std::string& node) {
  const bool was_present = nodes_.erase(node) > 0;
  if (!was_present) return;
  // Recompute exactly the points add_node placed, and erase only those we own.
  for (int i = 0; i < vnodes_; i++) {
    auto point = ring_.find(hash64(vnode_label(node, i)));
    if (point != ring_.end() && point->second == node) ring_.erase(point);
  }
}

const std::string& HashRing::node_for(std::string_view key) const {
  if (ring_.empty()) throw std::runtime_error("HashRing::node_for called on an empty ring");
  // The first point at or after the key's position is its owner...
  auto owner = ring_.lower_bound(hash64(key));
  // ...unless the key is past the last point, where the circle wraps to the first.
  if (owner == ring_.end()) owner = ring_.begin();
  return owner->second;
}

size_t HashRing::size() const { return nodes_.size(); }
