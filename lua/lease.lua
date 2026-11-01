-- Quota lease for hybrid mode.
-- KEYS[1] = rate-limit key   ARGV[1] = limit   ARGV[2] = window_ms   ARGV[3] = chunk
-- Returns {granted, ms_until_window_end}.
--
-- Instead of one Redis call per request, a gateway takes `chunk` requests'
-- worth of the current fixed window's quota and spends it locally. Redis
-- tracks how much of the window has been handed out, never more than `limit`.

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local chunk = tonumber(ARGV[3])

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)

local window = math.floor(now_ms / window_ms)
local window_key = key .. ':' .. window
local ms_until_window_end = (window + 1) * window_ms - now_ms

local handed_out = tonumber(redis.call('GET', window_key) or '0')
local granted = math.max(0, math.min(chunk, limit - handed_out))
if granted > 0 then
  redis.call('INCRBY', window_key, granted)
  redis.call('PEXPIRE', window_key, window_ms)
end

return {granted, ms_until_window_end}
