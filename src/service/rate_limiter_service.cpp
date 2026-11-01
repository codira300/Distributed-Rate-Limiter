#include "service/rate_limiter_service.hpp"

#include <chrono>
#include <utility>

namespace rl {

RateLimiterService::RateLimiterService(
    RateLimiterRegistry& registry,
    RedisClient& redis,
    Metrics& metrics,
    Logger& logger,
    bool fail_open,
    std::string instance_id)
    : registry_(registry),
      redis_(redis),
      metrics_(metrics),
      logger_(logger),
      fail_open_(fail_open),
      instance_id_(std::move(instance_id)) {}

Result<Decision> RateLimiterService::check(
    std::string_view client_id,
    std::string_view policy_id,
    std::optional<Algorithm> requested_algorithm,
    std::optional<std::uint32_t> cost) {
    const auto started = std::chrono::steady_clock::now();

    const RateLimiter* limiter = registry_.find_limiter(policy_id);
    if (limiter == nullptr) {
        metrics_.record_error(ErrorCode::unknown_policy);
        return Error{ErrorCode::unknown_policy, "unknown policy_id", false};
    }
    const Policy& policy = limiter->policy();

    if (requested_algorithm.has_value() && *requested_algorithm != policy.algorithm) {
        metrics_.record_error(ErrorCode::algorithm_mismatch);
        return Error{
            ErrorCode::algorithm_mismatch,
            "requested algorithm does not match the selected policy",
            false};
    }

    if (cost.has_value() && *cost == 0) {
        metrics_.record_error(ErrorCode::invalid_argument);
        return Error{ErrorCode::invalid_argument, "cost must be at least 1", false};
    }

    CheckRequest request;
    request.client_id = std::string(client_id);
    request.cost_tokens = cost.value_or(0);

    auto result = limiter->check(request);
    const auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - started);

    if (!result) {
        metrics_.record_error(result.error().code);
        logger_.warn(
            "rate_limit_error",
            "code=" + std::string(to_string(result.error().code)) +
                " policy=" + std::string(policy_id));

        if (fail_open_ && is_dependency_error(result.error().code)) {
            Decision decision;
            decision.allowed = true;
            decision.limit = policy_limit(policy);
            decision.remaining = 0;
            decision.retry_after = std::chrono::milliseconds(0);
            decision.reset_after = std::chrono::milliseconds(0);
            decision.algorithm = policy.algorithm;
            decision.policy_id = policy.id;
            decision.status = DecisionStatus::fail_open;
            metrics_.record_decision(decision.algorithm, decision.status, latency);
            logger_.warn("fail_open_decision", "policy=" + policy.id);
            return decision;
        }

        return result.error();
    }

    metrics_.record_decision(result.value().algorithm, result.value().status, latency);
    return result;
}

Result<bool> RateLimiterService::ready() {
    return redis_.ping();
}

} // namespace rl
