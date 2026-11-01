#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

#include "ratelimiter/decision.hpp"
#include "ratelimiter/errors.hpp"
#include "ratelimiter/policy.hpp"
#include "ratelimiter/redis_client.hpp"

namespace rl::detail {

[[nodiscard]] inline Result<Decision> decision_from_script_reply(
    const Result<ScriptReply>& reply,
    const Policy& policy,
    std::string_view context) {
    if (!reply) {
        return reply.error();
    }

    const auto& values = reply.value().integers;
    if (values.size() < 6) {
        return Error{
            ErrorCode::redis_protocol,
            std::string(context) + ": unexpected script reply size",
            true};
    }
    if (values[0] != 0 && values[0] != 1) {
        return Error{
            ErrorCode::redis_protocol,
            std::string(context) + ": invalid decision value",
            true};
    }

    Decision decision;
    decision.allowed = values[0] == 1;
    decision.limit = static_cast<std::uint64_t>(values[1]);
    decision.remaining = static_cast<std::uint64_t>(values[2]);
    decision.retry_after = std::chrono::milliseconds(values[3]);
    decision.reset_after = std::chrono::milliseconds(values[4]);
    decision.algorithm = policy.algorithm;
    decision.policy_id = policy.id;
    decision.status = decision.allowed ? DecisionStatus::allowed : DecisionStatus::rate_limited;
    return decision;
}

} // namespace rl::detail
