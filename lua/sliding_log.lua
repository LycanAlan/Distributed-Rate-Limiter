-- Sliding log, run atomically inside Redis (same rules as src/limiters.cpp).
-- KEYS[1] = rate-limit key   ARGV[1] = limit   ARGV[2] = window_ms
-- Returns {allowed (0|1), remaining, retry_after_ms}.
--
-- The log is a sorted set: score = request time, so dropping old entries is
-- one ZREMRANGEBYSCORE and counting the window is one ZCARD.

local key = KEYS[1]
local sequence_key = key .. ':seq'
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)

-- The window is (now - window_ms, now]: drop everything at or before its start.
redis.call('ZREMRANGEBYSCORE', key, '-inf', now_ms - window_ms)

local in_window = redis.call('ZCARD', key)
if in_window >= limit then
  -- A slot frees up the moment the oldest request leaves the window.
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  local oldest_ms = tonumber(oldest[2])
  return {0, 0, oldest_ms + window_ms - now_ms}
end

-- Set members must be unique: two requests in the same millisecond would
-- otherwise be stored as one. A counter gives each request its own name.
local sequence = redis.call('INCR', sequence_key)
redis.call('ZADD', key, now_ms, now_ms .. ':' .. sequence)
redis.call('PEXPIRE', key, window_ms)
redis.call('PEXPIRE', sequence_key, window_ms)

return {1, limit - in_window - 1, 0}
