#include "config/policy_registry.hpp"

#include <nlohmann/json.hpp>

#include <cmath>
#include <cstdint>
#include <fstream>
#include <set>
#include <stdexcept>
#include <string>

namespace rl {

namespace {

using json = nlohmann::json;

constexpr double kMicroPerToken = 1'000'000.0;

std::uint64_t required_unsigned(const json& object, const char* field) {
    if (!object.contains(field)) {
        throw std::invalid_argument(std::string("missing field: ") + field);
    }
    const auto& value = object.at(field);
    if (!value.is_number_integer() && !value.is_number_unsigned()) {
        throw std::invalid_argument(std::string(field) + " must be an integer");
    }
    if (value.is_number_integer() && value.get<std::int64_t>() < 0) {
        throw std::invalid_argument(std::string(field) + " must be non-negative");
    }
    return value.get<std::uint64_t>();
}

std::uint32_t optional_u32(const json& object, const char* field, std::uint32_t fallback) {
    if (!object.contains(field)) {
        return fallback;
    }
    const auto value = required_unsigned(object, field);
    if (value > 0xFFFF'FFFFULL) {
        throw std::invalid_argument(std::string(field) + " is too large");
    }
    return static_cast<std::uint32_t>(value);
}

std::uint64_t rate_to_micro(const json& object) {
    if (!object.contains("refill_tokens_per_second")) {
        throw std::invalid_argument("missing field: refill_tokens_per_second");
    }
    const auto& value = object.at("refill_tokens_per_second");
    if (!value.is_number()) {
        throw std::invalid_argument("refill_tokens_per_second must be a number");
    }
    const double rate = value.get<double>();
    if (!std::isfinite(rate) || rate <= 0.0) {
        throw std::invalid_argument("refill_tokens_per_second must be finite and positive");
    }
    const double micro = std::round(rate * kMicroPerToken);
    if (micro < 1.0 || micro > static_cast<double>(kMaxRefillMicrotokensPerSecond)) {
        throw std::invalid_argument("refill_tokens_per_second is outside the supported range");
    }
    return static_cast<std::uint64_t>(micro);
}

Policy parse_policy(const json& object) {
    if (!object.is_object()) {
        throw std::invalid_argument("each policy must be a JSON object");
    }
    if (!object.contains("id") || !object.at("id").is_string()) {
        throw std::invalid_argument("policy id must be a string");
    }
    if (!object.contains("algorithm") || !object.at("algorithm").is_string()) {
        throw std::invalid_argument("policy algorithm must be a string");
    }

    Policy policy;
    policy.id = object.at("id").get<std::string>();
    const auto algorithm = parse_algorithm(object.at("algorithm").get<std::string>());
    if (!algorithm) {
        throw std::invalid_argument("unknown algorithm for policy " + policy.id);
    }
    policy.algorithm = *algorithm;

    if (policy.algorithm == Algorithm::token_bucket) {
        const std::uint64_t capacity = required_unsigned(object, "capacity");
        const std::uint64_t rate = rate_to_micro(object);
        const auto default_cost = optional_u32(object, "default_cost", 1);
        const auto max_cost = optional_u32(object, "max_cost", default_cost);
        if (max_cost < default_cost) {
            throw std::invalid_argument("max_cost must be at least default_cost");
        }
        policy.parameters = TokenBucketPolicy{
            capacity,
            rate,
            default_cost,
            max_cost,
        };
    } else {
        const std::uint64_t limit = required_unsigned(object, "limit");
        const std::uint64_t window_ms = required_unsigned(object, "window_ms");
        if (window_ms > static_cast<std::uint64_t>(kMaxSlidingWindow.count())) {
            throw std::invalid_argument("window_ms exceeds 24 hours");
        }
        const auto default_cost = optional_u32(object, "default_cost", 1);
        const auto max_cost = optional_u32(object, "max_cost", default_cost);
        if (max_cost < default_cost) {
            throw std::invalid_argument("max_cost must be at least default_cost");
        }
        policy.parameters = SlidingWindowPolicy{
            limit,
            std::chrono::milliseconds(window_ms),
            default_cost,
            max_cost,
        };
    }

    if (const auto error = validate_policy(policy)) {
        throw std::invalid_argument(error->message);
    }
    return policy;
}

} // namespace

Result<std::vector<Policy>> load_policies_from_file(const std::string& path) {
    std::ifstream input(path);
    if (!input) {
        return Error{ErrorCode::invalid_policy, "cannot open policy file: " + path, false};
    }

    try {
        json root;
        input >> root;
        if (!root.is_object() || !root.contains("policies") || !root.at("policies").is_array()) {
            return Error{
                ErrorCode::invalid_policy,
                "policy file must contain an object with a policies array",
                false};
        }

        std::vector<Policy> policies;
        std::set<std::string> ids;
        for (const auto& entry : root.at("policies")) {
            Policy policy = parse_policy(entry);
            if (!ids.insert(policy.id).second) {
                return Error{ErrorCode::invalid_policy, "duplicate policy id: " + policy.id, false};
            }
            policies.push_back(std::move(policy));
        }

        if (policies.empty()) {
            return Error{ErrorCode::invalid_policy, "policy file contains no policies", false};
        }
        return policies;
    } catch (const std::exception& error) {
        return Error{
            ErrorCode::invalid_policy,
            "failed to parse policy file: " + std::string(error.what()),
            false};
    }
}

} // namespace rl
