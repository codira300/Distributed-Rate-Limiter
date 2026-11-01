#pragma once

#include <string>

#include "ratelimiter/policy.hpp"
#include "ratelimiter/rate_limiter.hpp"
#include "ratelimiter/redis_client.hpp"

namespace rl {

class SlidingWindowLimiter final : public RateLimiter {
public:
    SlidingWindowLimiter(RedisClient& redis, Policy policy, std::string key_prefix);

    [[nodiscard]] const Policy& policy() const noexcept override { return policy_; }
    [[nodiscard]] Result<Decision> check(const CheckRequest& request) const override;

private:
    RedisClient& redis_;
    Policy policy_;
    std::string key_prefix_;
};

} // namespace rl
