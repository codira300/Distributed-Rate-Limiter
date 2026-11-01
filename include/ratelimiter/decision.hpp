#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

#include "ratelimiter/policy.hpp"

namespace rl {

enum class DecisionStatus { allowed, rate_limited, fail_open };

inline std::string_view to_string(DecisionStatus status) noexcept {
    switch (status) {
    case DecisionStatus::allowed: return "allowed";
    case DecisionStatus::rate_limited: return "rate_limited";
    case DecisionStatus::fail_open: return "fail_open";
    }
    return "unknown";
}

struct Decision {
    bool allowed{false};
    std::uint64_t limit{0};
    std::uint64_t remaining{0};
    std::chrono::milliseconds retry_after{0};
    std::chrono::milliseconds reset_after{0};
    Algorithm algorithm{Algorithm::token_bucket};
    std::string policy_id;
    DecisionStatus status{DecisionStatus::rate_limited};
};

} // namespace rl
