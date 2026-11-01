-- ratelimiter token bucket script, version 1
-- KEYS[1] bucket hash
-- ARGV[1] capacity in microtokens
-- ARGV[2] refill rate in microtokens per second (> 0)
-- ARGV[3] request cost in microtokens
-- ARGV[4] idle TTL in milliseconds when the bucket is full
-- Returns: allowed, limit, remaining, retry_after_ms, reset_after_ms, reserved

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local idle_ttl = tonumber(ARGV[4])

local now_parts = redis.call('TIME')
local now_ms = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)

local state = redis.call('HMGET', key, 'tokens_u', 'last_ms')
local tokens = tonumber(state[1])
local last_ms = tonumber(state[2])

if tokens == nil or last_ms == nil then
    tokens = capacity
    last_ms = now_ms
end

tokens = math.max(0, math.min(capacity, tokens))

local elapsed = now_ms - last_ms
if elapsed < 0 then
    elapsed = 0
end

if rate > 0 and tokens < capacity and elapsed > 0 then
    local needed = capacity - tokens
    local max_elapsed = math.ceil(needed * 1000 / rate)
    local effective_elapsed = elapsed
    if effective_elapsed > max_elapsed then
        effective_elapsed = max_elapsed
    end

    local added = math.floor(effective_elapsed * rate / 1000)
    if added > 0 then
        tokens = tokens + added
        if tokens >= capacity then
            tokens = capacity
            last_ms = now_ms
        else
            local consumed_ms = math.floor(added * 1000 / rate)
            last_ms = last_ms + consumed_ms
            if last_ms > now_ms then
                last_ms = now_ms
            end
        end
    end
elseif tokens >= capacity then
    last_ms = now_ms
end

local allowed = 0
local retry_after = 0

if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
elseif rate > 0 then
    retry_after = math.ceil((cost - tokens) * 1000 / rate)
    if retry_after < 1 then
        retry_after = 1
    end
end

local ttl
if tokens >= capacity or rate <= 0 then
    ttl = idle_ttl
else
    ttl = math.ceil((capacity - tokens) * 1000 / rate) + 1000
    if ttl < 1000 then
        ttl = 1000
    end
end

redis.call('HSET', key, 'tokens_u', tokens, 'last_ms', last_ms)
redis.call('PEXPIRE', key, ttl)

local reset_after = 0
if tokens < capacity and rate > 0 then
    reset_after = math.ceil((capacity - tokens) * 1000 / rate)
end

return {
    allowed,
    math.floor(capacity / 1000000),
    math.floor(tokens / 1000000),
    retry_after,
    reset_after,
    0
}
