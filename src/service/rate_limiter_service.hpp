#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "ratelimiter/decision.hpp"
#include "ratelimiter/errors.hpp"
#include "ratelimiter/logger.hpp"
#include "ratelimiter/metrics.hpp"
#include "ratelimiter/policy.hpp"
#include "ratelimiter/rate_limiter.hpp"
#include "ratelimiter/redis_client.hpp"

namespace rl {

class RateLimiterService {
public:
    RateLimiterService(
        RateLimiterRegistry& registry,
        RedisClient& redis,
        Metrics& metrics,
        Logger& logger,
        bool fail_open,
        std::string instance_id);

    [[nodiscard]] Result<Decision> check(
        std::string_view client_id,
        std::string_view policy_id,
        std::optional<Algorithm> requested_algorithm,
        std::optional<std::uint32_t> cost);

    [[nodiscard]] Result<bool> ready();
    [[nodiscard]] const std::string& instance_id() const noexcept { return instance_id_; }
    [[nodiscard]] bool fail_open_enabled() const noexcept { return fail_open_; }

private:
    RateLimiterRegistry& registry_;
    RedisClient& redis_;
    Metrics& metrics_;
    Logger& logger_;
    bool fail_open_;
    std::string instance_id_;
};

} // namespace rl
