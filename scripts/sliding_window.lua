-- ratelimiter sliding window log script, version 1
-- KEYS[1] sorted-set log
-- KEYS[2] sequence counter (declared for Redis Cluster hash-slot safety)
-- ARGV[1] window in milliseconds
-- ARGV[2] maximum request units in the window
-- ARGV[3] request cost in request units
-- Returns: allowed, limit, remaining, retry_after_ms, reset_after_ms, cost

local log_key = KEYS[1]
local seq_key = KEYS[2]
local window_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local now_parts = redis.call('TIME')
local now_ms = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
local cutoff = now_ms - window_ms

if cost <= 0 or cost > limit then
    return { -1, limit, 0, 0, 0, cost }
end

redis.call('ZREMRANGEBYSCORE', log_key, '-inf', cutoff)
local count = redis.call('ZCARD', log_key)

local allowed = 0
local retry_after = 0

if count + cost <= limit then
    local last_seq = redis.call('INCRBY', seq_key, cost)
    local first_seq = last_seq - cost + 1
    for seq = first_seq, last_seq do
        redis.call('ZADD', log_key, now_ms, tostring(now_ms) .. ':' .. tostring(seq))
    end
    count = count + cost
    allowed = 1
else
    local needed = count + cost - limit
    local oldest = redis.call('ZRANGE', log_key, needed - 1, needed - 1, 'WITHSCORES')
    if oldest[2] ~= nil then
        retry_after = tonumber(oldest[2]) + window_ms - now_ms
        if retry_after < 1 then
            retry_after = 1
        end
    end
end

redis.call('PEXPIRE', log_key, window_ms + 1000)
redis.call('PEXPIRE', seq_key, window_ms + 61000)

local reset_after = 0
if count > 0 then
    local newest = redis.call('ZRANGE', log_key, -1, -1, 'WITHSCORES')
    if newest[2] ~= nil then
        reset_after = tonumber(newest[2]) + window_ms - now_ms
        if reset_after < 0 then
            reset_after = 0
        end
    end
end

local remaining = limit - count
if remaining < 0 then
    remaining = 0
end

return { allowed, limit, remaining, retry_after, reset_after, cost }
