#include "ratelimiter/token_bucket.hpp"

#include <stdexcept>
#include <string>
#include <vector>

#include "lua_scripts.hpp"
#include "script_reply.hpp"

namespace rl {

namespace {

constexpr std::uint64_t kMicroPerToken = 1'000'000;

} // namespace

TokenBucketLimiter::TokenBucketLimiter(
    RedisClient& redis,
    Policy policy,
    std::string key_prefix,
    std::chrono::milliseconds idle_ttl)
    : redis_(redis),
      policy_(std::move(policy)),
      key_prefix_(std::move(key_prefix)),
      idle_ttl_(idle_ttl) {
    if (policy_.algorithm != Algorithm::token_bucket) {
        throw std::invalid_argument("TokenBucketLimiter requires a token_bucket policy");
    }
    if (const auto error = validate_policy(policy_)) {
        throw std::invalid_argument("invalid token bucket policy: " + error->message);
    }
    if (const auto error = validate_key_prefix(key_prefix_)) {
        throw std::invalid_argument("invalid Redis key prefix: " + error->message);
    }
    if (idle_ttl_.count() <= 0) {
        throw std::invalid_argument("idle TTL must be positive");
    }
}

Result<Decision> TokenBucketLimiter::check(const CheckRequest& request) const {
    if (const auto error = validate_client_id(request.client_id)) {
        return *error;
    }

    const std::uint32_t cost = request.cost_tokens == 0
        ? as_token_bucket(policy_).default_cost_tokens
        : request.cost_tokens;

    if (const auto error = validate_cost(policy_, cost)) {
        return *error;
    }

    const auto& params = as_token_bucket(policy_);
    const std::string key = key_prefix_ + ":tb:{" + policy_.id + ":" + request.client_id + "}";

    const std::vector<std::string> keys{key};
    const std::vector<std::string> args{
        std::to_string(params.capacity_tokens * kMicroPerToken),
        std::to_string(params.refill_microtokens_per_second),
        std::to_string(static_cast<std::uint64_t>(cost) * kMicroPerToken),
        std::to_string(idle_ttl_.count()),
    };

    const auto reply = redis_.eval_script(
        scripts::kTokenBucketName,
        scripts::kTokenBucket,
        keys,
        args);
    return detail::decision_from_script_reply(reply, policy_, "token bucket");
}

} // namespace rl
