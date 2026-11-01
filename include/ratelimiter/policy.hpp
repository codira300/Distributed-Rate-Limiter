#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "ratelimiter/errors.hpp"

namespace rl {

inline constexpr std::uint64_t kMaxPolicyCapacityTokens = 1'000'000;
inline constexpr std::uint64_t kMaxRefillMicrotokensPerSecond = 1'000'000'000'000'000ULL;
inline constexpr std::uint32_t kMaxRequestCostTokens = 10'000;
inline constexpr std::uint64_t kMaxSlidingWindowLimit = 1'000'000;
inline constexpr std::chrono::milliseconds kMaxSlidingWindow{86'400'000};
inline constexpr std::size_t kMaxClientIdLength = 128;
inline constexpr std::size_t kMaxPolicyIdLength = 64;

enum class Algorithm { token_bucket, sliding_window };

inline std::string_view to_string(Algorithm algorithm) noexcept {
    switch (algorithm) {
    case Algorithm::token_bucket: return "token_bucket";
    case Algorithm::sliding_window: return "sliding_window";
    }
    return "unknown";
}

std::optional<Algorithm> parse_algorithm(std::string_view value) noexcept;

struct TokenBucketPolicy {
    std::uint64_t capacity_tokens{0};
    std::uint64_t refill_microtokens_per_second{0};
    std::uint32_t default_cost_tokens{1};
    std::uint32_t max_cost_tokens{1};
};

struct SlidingWindowPolicy {
    std::uint64_t limit_requests{0};
    std::chrono::milliseconds window{0};
    std::uint32_t default_cost_tokens{1};
    std::uint32_t max_cost_tokens{1};
};

struct Policy {
    std::string id;
    Algorithm algorithm{Algorithm::token_bucket};
    std::variant<TokenBucketPolicy, SlidingWindowPolicy> parameters;
};

[[nodiscard]] const TokenBucketPolicy& as_token_bucket(const Policy& policy);
[[nodiscard]] const SlidingWindowPolicy& as_sliding_window(const Policy& policy);

[[nodiscard]] std::uint64_t policy_limit(const Policy& policy) noexcept;
[[nodiscard]] std::uint32_t policy_default_cost(const Policy& policy) noexcept;
[[nodiscard]] std::uint32_t policy_max_cost(const Policy& policy) noexcept;

[[nodiscard]] std::optional<Error> validate_policy(const Policy& policy);
[[nodiscard]] std::optional<Error> validate_client_id(std::string_view client_id) noexcept;
[[nodiscard]] std::optional<Error> validate_policy_id(std::string_view policy_id) noexcept;
[[nodiscard]] std::optional<Error> validate_key_prefix(std::string_view prefix) noexcept;
[[nodiscard]] std::optional<Error> validate_cost(const Policy& policy, std::uint32_t cost) noexcept;

} // namespace rl
