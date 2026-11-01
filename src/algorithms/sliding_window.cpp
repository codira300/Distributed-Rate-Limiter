#include "ratelimiter/sliding_window.hpp"

#include <stdexcept>
#include <string>
#include <vector>

#include "lua_scripts.hpp"
#include "script_reply.hpp"

namespace rl {

SlidingWindowLimiter::SlidingWindowLimiter(
    RedisClient& redis,
    Policy policy,
    std::string key_prefix)
    : redis_(redis),
      policy_(std::move(policy)),
      key_prefix_(std::move(key_prefix)) {
    if (policy_.algorithm != Algorithm::sliding_window) {
        throw std::invalid_argument("SlidingWindowLimiter requires a sliding_window policy");
    }
    if (const auto error = validate_policy(policy_)) {
        throw std::invalid_argument("invalid sliding window policy: " + error->message);
    }
    if (const auto error = validate_key_prefix(key_prefix_)) {
        throw std::invalid_argument("invalid Redis key prefix: " + error->message);
    }
}

Result<Decision> SlidingWindowLimiter::check(const CheckRequest& request) const {
    if (const auto error = validate_client_id(request.client_id)) {
        return *error;
    }

    const std::uint32_t cost = request.cost_tokens == 0
        ? as_sliding_window(policy_).default_cost_tokens
        : request.cost_tokens;

    if (const auto error = validate_cost(policy_, cost)) {
        return *error;
    }

    const auto& params = as_sliding_window(policy_);
    const std::string hash_tag = "{" + policy_.id + ":" + request.client_id + "}";
    const std::string log_key = key_prefix_ + ":sw:" + hash_tag + ":log";
    const std::string seq_key = key_prefix_ + ":sw:" + hash_tag + ":seq";

    const std::vector<std::string> keys{log_key, seq_key};
    const std::vector<std::string> args{
        std::to_string(params.window.count()),
        std::to_string(params.limit_requests),
        std::to_string(cost),
    };

    const auto reply = redis_.eval_script(
        scripts::kSlidingWindowName,
        scripts::kSlidingWindow,
        keys,
        args);
    if (!reply) {
        return reply.error();
    }
    if (!reply.value().integers.empty() && reply.value().integers[0] == -1) {
        return Error{ErrorCode::internal, "sliding window script rejected the request cost", false};
    }
    return detail::decision_from_script_reply(reply, policy_, "sliding window");
}

} // namespace rl
