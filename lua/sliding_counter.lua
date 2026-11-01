-- Sliding window counter, run atomically inside Redis (same rules as src/limiters.cpp).
-- KEYS[1] = rate-limit key   ARGV[1] = limit   ARGV[2] = window_ms
-- Returns {allowed (0|1), remaining, retry_after_ms}.
--
-- Two counters, the current fixed window and the one before it. The previous
-- one counts in proportion to how much of it still overlaps the sliding window:
--   previous * (W - elapsed) + current * W  <  limit * W

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)

local window = math.floor(now_ms / window_ms)
local elapsed_ms = now_ms - window * window_ms
local current_key = key .. ':' .. window
local previous_key = key .. ':' .. (window - 1)

-- GET of a missing key gives false in Lua; a missing window counts as 0.
local current = tonumber(redis.call('GET', current_key) or '0')
local previous = tonumber(redis.call('GET', previous_key) or '0')

local previous_weighted = previous * (window_ms - elapsed_ms)
local budget = limit * window_ms

-- The earliest elapsed time e in [0, W] at which fading * (W - e) < room.
local function first_moment_under(fading, room)
  if fading == 0 then return 0 end
  local e = window_ms - math.ceil(room / fading) + 1
  return math.max(0, math.min(window_ms, e))
end

if previous_weighted + current * window_ms >= budget then
  -- Either later in this window as the previous count fades, or in the next
  -- window where this window's count is the one fading.
  local room_this_window = (limit - current) * window_ms
  if room_this_window > 0 then
    local at = first_moment_under(previous, room_this_window)
    if at < window_ms then return {0, 0, at - elapsed_ms} end
  end
  local into_next_window = first_moment_under(current, budget)
  return {0, 0, (window_ms - elapsed_ms) + into_next_window}
end

redis.call('INCR', current_key)
-- Keep it for two windows: in the next one it is still needed as "previous".
redis.call('PEXPIRE', current_key, 2 * window_ms)

local room = budget - previous_weighted - (current + 1) * window_ms
return {1, math.max(0, math.floor(room / window_ms)), 0}
