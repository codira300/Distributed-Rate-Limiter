#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "ratelimiter/decision.hpp"
#include "ratelimiter/errors.hpp"
#include "ratelimiter/policy.hpp"

namespace rl {

struct CheckRequest {
    std::string client_id;
    std::uint32_t cost_tokens{0};
};

class RateLimiter {
public:
    virtual ~RateLimiter() = default;

    [[nodiscard]] virtual const Policy& policy() const noexcept = 0;
    [[nodiscard]] virtual Result<Decision> check(const CheckRequest& request) const = 0;
};

class RateLimiterRegistry {
public:
    void add(std::unique_ptr<RateLimiter> limiter);
    void seal() noexcept { sealed_ = true; }

    [[nodiscard]] Result<Decision> check(
        std::string_view policy_id,
        const CheckRequest& request) const;

    [[nodiscard]] const RateLimiter* find_limiter(std::string_view policy_id) const noexcept;
    [[nodiscard]] const Policy* find_policy(std::string_view policy_id) const noexcept;
    [[nodiscard]] std::size_t size() const noexcept { return limiters_.size(); }

private:
    struct StringHash {
        using is_transparent = void;

        [[nodiscard]] std::size_t operator()(std::string_view value) const noexcept {
            return std::hash<std::string_view>{}(value);
        }
        [[nodiscard]] std::size_t operator()(const std::string& value) const noexcept {
            return std::hash<std::string_view>{}(value);
        }
    };

    std::unordered_map<
        std::string,
        std::unique_ptr<RateLimiter>,
        StringHash,
        std::equal_to<>> limiters_;
    bool sealed_{false};
};

} // namespace rl
