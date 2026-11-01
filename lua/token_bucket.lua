-- Token bucket, run atomically inside Redis (same rules as src/limiters.cpp).
-- KEYS[1] = rate-limit key   ARGV[1] = limit (capacity)   ARGV[2] = window_ms
-- Refill: limit tokens per window_ms. Returns {allowed (0|1), remaining, retry_after_ms}.
--
-- Stored as a hash {tokens, ts}. Refill is lazy: tokens earned since ts are
-- added on each call, so no timer runs anywhere.

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)

local stored = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(stored[1])
local last_ms = tonumber(stored[2])
if tokens == nil then
  -- No bucket yet, or it expired. Either way it is full.
  tokens = limit
  last_ms = now_ms
end

local earned = (now_ms - last_ms) * limit / window_ms
tokens = math.min(limit, tokens + earned)

local allowed = 0
local retry_after_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_after_ms = math.ceil((1 - tokens) * window_ms / limit)
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now_ms)
-- An untouched bucket is full again after window_ms, the same as no bucket at all.
redis.call('PEXPIRE', key, window_ms)

return {allowed, math.floor(tokens), retry_after_ms}
