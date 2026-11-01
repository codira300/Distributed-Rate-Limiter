#pragma once

#include <chrono>
#include <string>

#include "ratelimiter/policy.hpp"
#include "ratelimiter/rate_limiter.hpp"
#include "ratelimiter/redis_client.hpp"

namespace rl {

class TokenBucketLimiter final : public RateLimiter {
public:
    TokenBucketLimiter(
        RedisClient& redis,
        Policy policy,
        std::string key_prefix,
        std::chrono::milliseconds idle_ttl);

    [[nodiscard]] const Policy& policy() const noexcept override { return policy_; }
    [[nodiscard]] Result<Decision> check(const CheckRequest& request) const override;

private:
    RedisClient& redis_;
    Policy policy_;
    std::string key_prefix_;
    std::chrono::milliseconds idle_ttl_;
};

} // namespace rl
