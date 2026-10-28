#pragma once
#include <cstdint>

struct Decision {
  bool allowed;
  int64_t remaining;       // requests still allowed right now, >= 0
  int64_t retry_after_ms;  // 0 if allowed; else ms until a request could pass
};

enum class Algo { FixedWindow, SlidingLog, SlidingCounter, TokenBucket };
