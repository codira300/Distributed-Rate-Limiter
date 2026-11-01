#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "ratelimiter/errors.hpp"

namespace rl {

struct RedisConfig {
    std::string host{"127.0.0.1"};
    int port{6379};
    std::optional<std::string> password;
    int database{0};
    std::chrono::milliseconds connect_timeout{500};
    std::chrono::milliseconds command_timeout{1000};
    std::chrono::milliseconds acquire_timeout{500};
    std::size_t pool_size{16};
};

struct RedisStats {
    std::size_t pool_size{0};
    std::size_t idle{0};
    std::size_t in_use{0};
    std::uint64_t connections_created{0};
    std::uint64_t connection_failures{0};
    std::uint64_t commands{0};
    std::uint64_t command_errors{0};
    std::uint64_t noscript_recoveries{0};
    std::uint64_t pool_acquire_timeouts{0};
};

struct ScriptReply {
    std::vector<std::int64_t> integers;
};

class RedisClient {
public:
    explicit RedisClient(RedisConfig config);
    ~RedisClient();

    RedisClient(const RedisClient&) = delete;
    RedisClient& operator=(const RedisClient&) = delete;
    RedisClient(RedisClient&&) noexcept;
    RedisClient& operator=(RedisClient&&) noexcept;

    [[nodiscard]] Result<ScriptReply> eval_script(
        std::string_view script_name,
        std::string_view script_body,
        const std::vector<std::string>& keys,
        const std::vector<std::string>& args);

    [[nodiscard]] Result<bool> ping();

    [[nodiscard]] RedisStats stats() const;
    [[nodiscard]] const RedisConfig& config() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace rl
