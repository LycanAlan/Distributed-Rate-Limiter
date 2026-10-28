#include <gtest/gtest.h>

#include <bitset>
#include <map>
#include <vector>

#include "hash_ring.h"

static std::string K(int i) { return "rl:api:user" + std::to_string(i); }

TEST(Hash64, IsDeterministicAndAvalanches) {
  EXPECT_EQ(hash64("abc"), hash64("abc"));
  double bits = 0;
  for (int i = 0; i < 1000; i++)
    bits += std::bitset<64>(hash64("node#" + std::to_string(i)) ^
                            hash64("node#" + std::to_string(i + 1)))
                .count();
  bits /= 1000;
  EXPECT_GT(bits, 28.0);  // good mixing flips about 32 of 64 bits
  EXPECT_LT(bits, 36.0);
}

TEST(HashRing, EmptyRingThrows) {
  HashRing r;
  EXPECT_THROW(r.node_for("x"), std::runtime_error);
}

TEST(HashRing, SingleNodeOwnsEverything) {
  HashRing r;
  r.add_node("a");
  for (int i = 0; i < 1000; i++) EXPECT_EQ(r.node_for(K(i)), "a");
}

TEST(HashRing, SizeCountsPhysicalNodes) {
  HashRing r;
  r.add_node("a");
  r.add_node("b");
  r.add_node("a");
  EXPECT_EQ(r.size(), 2u);
  r.remove_node("b");
  r.remove_node("zzz");
  EXPECT_EQ(r.size(), 1u);
}

TEST(HashRing, InsertionOrderDoesNotMatter) {
  HashRing x, y;
  for (auto n : {"a", "b", "c"}) x.add_node(n);
  for (auto n : {"c", "a", "b"}) y.add_node(n);
  for (int i = 0; i < 5000; i++) EXPECT_EQ(x.node_for(K(i)), y.node_for(K(i)));
}

TEST(HashRing, DistributionIsReasonablyEven) {
  HashRing r;
  for (auto n : {"a", "b", "c"}) r.add_node(n);
  std::map<std::string, int> count;
  for (int i = 0; i < 30000; i++) count[r.node_for(K(i))]++;
  ASSERT_EQ(count.size(), 3u);
  for (auto& [node, n] : count) {
    EXPECT_GT(n, 6000) << node;   // > 20%
    EXPECT_LT(n, 14100) << node;  // < 47%
  }
}

TEST(HashRing, AddingANodeMovesAboutAQuarterAllToTheNewNode) {
  HashRing r;
  for (auto n : {"a", "b", "c"}) r.add_node(n);
  std::vector<std::string> before;
  for (int i = 0; i < 30000; i++) before.push_back(r.node_for(K(i)));
  r.add_node("d");
  int moved = 0;
  for (int i = 0; i < 30000; i++) {
    const std::string& now = r.node_for(K(i));
    if (now != before[i]) {
      moved++;
      EXPECT_EQ(now, "d");
    }
  }
  EXPECT_GT(moved, 4500);   // > 15%
  EXPECT_LT(moved, 10500);  // < 35%   (ideal 25%)
}

TEST(HashRing, RemovingANodeMovesOnlyItsKeys) {
  HashRing r;
  for (auto n : {"a", "b", "c", "d"}) r.add_node(n);
  std::vector<std::string> before;
  for (int i = 0; i < 30000; i++) before.push_back(r.node_for(K(i)));
  r.remove_node("b");
  for (int i = 0; i < 30000; i++) {
    if (before[i] != "b") EXPECT_EQ(r.node_for(K(i)), before[i]);
  }
}
