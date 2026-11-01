#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace rl {

enum class ErrorCode {
    invalid_argument,
    invalid_client_id,
    invalid_policy,
    unknown_policy,
    algorithm_mismatch,
    cost_exceeds_limit,
    redis_unavailable,
    redis_timeout,
    redis_protocol,
    pool_exhausted,
    internal,
};

inline constexpr std::size_t kErrorCodeCount =
    static_cast<std::size_t>(ErrorCode::internal) + 1;

struct Error {
    ErrorCode code{ErrorCode::internal};
    std::string message;
    bool retryable{false};
};

inline std::string_view to_string(ErrorCode code) noexcept {
    switch (code) {
    case ErrorCode::invalid_argument: return "invalid_argument";
    case ErrorCode::invalid_client_id: return "invalid_client_id";
    case ErrorCode::invalid_policy: return "invalid_policy";
    case ErrorCode::unknown_policy: return "unknown_policy";
    case ErrorCode::algorithm_mismatch: return "algorithm_mismatch";
    case ErrorCode::cost_exceeds_limit: return "cost_exceeds_limit";
    case ErrorCode::redis_unavailable: return "redis_unavailable";
    case ErrorCode::redis_timeout: return "redis_timeout";
    case ErrorCode::redis_protocol: return "redis_protocol";
    case ErrorCode::pool_exhausted: return "pool_exhausted";
    case ErrorCode::internal: return "internal";
    }
    return "internal";
}

inline bool is_dependency_error(ErrorCode code) noexcept {
    return code == ErrorCode::redis_unavailable ||
           code == ErrorCode::redis_timeout ||
           code == ErrorCode::redis_protocol ||
           code == ErrorCode::pool_exhausted;
}

template <typename T>
class Result {
public:
    Result(T value) : storage_(std::move(value)) {}
    Result(Error error) : storage_(std::move(error)) {}

    [[nodiscard]] bool has_value() const noexcept {
        return std::holds_alternative<T>(storage_);
    }

    explicit operator bool() const noexcept { return has_value(); }

    [[nodiscard]] T& value() & { return std::get<T>(storage_); }
    [[nodiscard]] const T& value() const& { return std::get<T>(storage_); }
    [[nodiscard]] T&& value() && { return std::get<T>(std::move(storage_)); }

    [[nodiscard]] Error& error() & { return std::get<Error>(storage_); }
    [[nodiscard]] const Error& error() const& { return std::get<Error>(storage_); }
    [[nodiscard]] Error&& error() && { return std::get<Error>(std::move(storage_)); }

private:
    std::variant<T, Error> storage_;
};

} // namespace rl
