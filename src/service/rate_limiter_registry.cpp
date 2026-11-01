#include "ratelimiter/rate_limiter.hpp"

#include <stdexcept>
#include <utility>

namespace rl {

void RateLimiterRegistry::add(std::unique_ptr<RateLimiter> limiter) {
    if (sealed_) {
        throw std::logic_error("cannot add a limiter after the registry is sealed");
    }
    if (!limiter) {
        throw std::invalid_argument("limiter must not be null");
    }

    const std::string id = limiter->policy().id;
    if (id.empty()) {
        throw std::invalid_argument("limiter policy id must not be empty");
    }
    const bool inserted = limiters_.try_emplace(id, std::move(limiter)).second;
    if (!inserted) {
        throw std::invalid_argument("duplicate policy id: " + id);
    }
}

Result<Decision> RateLimiterRegistry::check(
    std::string_view policy_id,
    const CheckRequest& request) const {
    const RateLimiter* limiter = find_limiter(policy_id);
    if (limiter == nullptr) {
        return Error{
            ErrorCode::unknown_policy,
            "unknown policy_id: " + std::string(policy_id),
            false};
    }
    return limiter->check(request);
}

const RateLimiter* RateLimiterRegistry::find_limiter(std::string_view policy_id) const noexcept {
    const auto it = limiters_.find(policy_id);
    return it == limiters_.end() ? nullptr : it->second.get();
}

const Policy* RateLimiterRegistry::find_policy(std::string_view policy_id) const noexcept {
    const RateLimiter* limiter = find_limiter(policy_id);
    return limiter == nullptr ? nullptr : &limiter->policy();
}

} // namespace rl
