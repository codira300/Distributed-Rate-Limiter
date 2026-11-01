#include "ratelimiter/policy.hpp"

#include <string>

namespace rl {

namespace {

bool is_identity_char(char ch) noexcept {
    const auto c = static_cast<unsigned char>(ch);
    if (c >= '0' && c <= '9') return true;
    if (c >= 'A' && c <= 'Z') return true;
    if (c >= 'a' && c <= 'z') return true;
    return ch == '.' || ch == '_' || ch == '-';
}

std::optional<Error> validate_costs(
    std::uint32_t default_cost,
    std::uint32_t max_cost,
    std::uint64_t limit) {
    if (max_cost == 0 || max_cost > kMaxRequestCostTokens) {
        return Error{ErrorCode::invalid_policy, "max_cost must be between 1 and 10000", false};
    }
    if (default_cost == 0 || default_cost > max_cost) {
        return Error{ErrorCode::invalid_policy, "default_cost must be between 1 and max_cost", false};
    }
    if (static_cast<std::uint64_t>(max_cost) > limit) {
        return Error{ErrorCode::invalid_policy, "max_cost must not exceed the policy capacity/limit", false};
    }
    return std::nullopt;
}

} // namespace

std::optional<Algorithm> parse_algorithm(std::string_view value) noexcept {
    if (value == "token_bucket") return Algorithm::token_bucket;
    if (value == "sliding_window") return Algorithm::sliding_window;
    return std::nullopt;
}

const TokenBucketPolicy& as_token_bucket(const Policy& policy) {
    return std::get<TokenBucketPolicy>(policy.parameters);
}

const SlidingWindowPolicy& as_sliding_window(const Policy& policy) {
    return std::get<SlidingWindowPolicy>(policy.parameters);
}

std::uint64_t policy_limit(const Policy& policy) noexcept {
    if (const auto* params = std::get_if<TokenBucketPolicy>(&policy.parameters)) {
        return params->capacity_tokens;
    }
    if (const auto* params = std::get_if<SlidingWindowPolicy>(&policy.parameters)) {
        return params->limit_requests;
    }
    return 0;
}

std::uint32_t policy_default_cost(const Policy& policy) noexcept {
    if (const auto* params = std::get_if<TokenBucketPolicy>(&policy.parameters)) {
        return params->default_cost_tokens;
    }
    if (const auto* params = std::get_if<SlidingWindowPolicy>(&policy.parameters)) {
        return params->default_cost_tokens;
    }
    return 1;
}

std::uint32_t policy_max_cost(const Policy& policy) noexcept {
    if (const auto* params = std::get_if<TokenBucketPolicy>(&policy.parameters)) {
        return params->max_cost_tokens;
    }
    if (const auto* params = std::get_if<SlidingWindowPolicy>(&policy.parameters)) {
        return params->max_cost_tokens;
    }
    return 1;
}

std::optional<Error> validate_policy(const Policy& policy) {
    if (const auto id_error = validate_policy_id(policy.id)) {
        return id_error;
    }

    if (policy.algorithm == Algorithm::token_bucket) {
        const auto& params = as_token_bucket(policy);
        if (params.capacity_tokens == 0 || params.capacity_tokens > kMaxPolicyCapacityTokens) {
            return Error{ErrorCode::invalid_policy, "capacity_tokens must be between 1 and 1000000", false};
        }
        if (params.refill_microtokens_per_second == 0 ||
            params.refill_microtokens_per_second > kMaxRefillMicrotokensPerSecond) {
            return Error{ErrorCode::invalid_policy, "refill rate is outside the supported range", false};
        }
        return validate_costs(
            params.default_cost_tokens,
            params.max_cost_tokens,
            params.capacity_tokens);
    }

    const auto& params = as_sliding_window(policy);
    if (params.limit_requests == 0 || params.limit_requests > kMaxSlidingWindowLimit) {
        return Error{ErrorCode::invalid_policy, "limit_requests must be between 1 and 1000000", false};
    }
    if (params.window.count() <= 0 || params.window > kMaxSlidingWindow) {
        return Error{ErrorCode::invalid_policy, "window must be between 1ms and 24h", false};
    }
    return validate_costs(
        params.default_cost_tokens,
        params.max_cost_tokens,
        params.limit_requests);
}

std::optional<Error> validate_client_id(std::string_view client_id) noexcept {
    if (client_id.empty()) {
        return Error{ErrorCode::invalid_client_id, "client_id must not be empty", false};
    }
    if (client_id.size() > kMaxClientIdLength) {
        return Error{ErrorCode::invalid_client_id, "client_id exceeds 128 characters", false};
    }
    for (const char ch : client_id) {
        if (!is_identity_char(ch)) {
            return Error{
                ErrorCode::invalid_client_id,
                "client_id may contain only ASCII letters, digits, dot, underscore, and hyphen",
                false};
        }
    }
    return std::nullopt;
}

std::optional<Error> validate_policy_id(std::string_view policy_id) noexcept {
    if (policy_id.empty()) {
        return Error{ErrorCode::invalid_policy, "policy id must not be empty", false};
    }
    if (policy_id.size() > kMaxPolicyIdLength) {
        return Error{ErrorCode::invalid_policy, "policy id exceeds 64 characters", false};
    }
    for (const char ch : policy_id) {
        if (!is_identity_char(ch)) {
            return Error{
                ErrorCode::invalid_policy,
                "policy id may contain only ASCII letters, digits, dot, underscore, and hyphen",
                false};
        }
    }
    return std::nullopt;
}

std::optional<Error> validate_key_prefix(std::string_view prefix) noexcept {
    if (prefix.empty() || prefix.size() > 128) {
        return Error{
            ErrorCode::invalid_argument,
            "Redis key prefix must be between 1 and 128 characters",
            false};
    }
    for (const char ch : prefix) {
        if (ch == '{' || ch == '}' || ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') {
            return Error{
                ErrorCode::invalid_argument,
                "Redis key prefix must not contain braces or whitespace",
                false};
        }
    }
    return std::nullopt;
}

std::optional<Error> validate_cost(const Policy& policy, std::uint32_t cost) noexcept {
    if (cost == 0) {
        return Error{ErrorCode::invalid_argument, "cost must be at least 1", false};
    }
    if (cost > policy_max_cost(policy)) {
        return Error{ErrorCode::cost_exceeds_limit, "cost exceeds the policy max_cost", false};
    }
    if (static_cast<std::uint64_t>(cost) > policy_limit(policy)) {
        return Error{ErrorCode::cost_exceeds_limit, "cost exceeds the policy capacity/limit", false};
    }
    return std::nullopt;
}

} // namespace rl
