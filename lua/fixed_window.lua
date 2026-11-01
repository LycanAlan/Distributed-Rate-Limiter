-- Fixed window, run atomically inside Redis (same rules as src/limiters.cpp).
-- KEYS[1] = rate-limit key   ARGV[1] = limit   ARGV[2] = window_ms
-- Returns {allowed (0|1), remaining, retry_after_ms}.
--
-- Why a script: Redis runs it start to finish with no other command in
-- between, so "increment, then compare with the limit" cannot interleave with
-- another gateway doing the same. That interleaving is the race naive mode shows.

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])

-- Redis's clock, not the gateway's: every node then agrees on window edges.
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)

local window = math.floor(now_ms / window_ms)
local window_key = key .. ':' .. window

-- INCR first and check after: one command instead of GET + compare + SET.
local count = redis.call('INCR', window_key)
if count == 1 then
  redis.call('PEXPIRE', window_key, window_ms)  -- the count is useless once its window is over
end

if count > limit then
  local window_end_ms = (window + 1) * window_ms
  return {0, 0, window_end_ms - now_ms}
end
return {1, limit - count, 0}
