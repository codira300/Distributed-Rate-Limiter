#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "ratelimiter/decision.hpp"
#include "ratelimiter/errors.hpp"
#include "ratelimiter/redis_client.hpp"

namespace rl {

class Metrics {
public:
    Metrics();
    ~Metrics();

    Metrics(const Metrics&) = delete;
    Metrics& operator=(const Metrics&) = delete;

    void record_decision(
        Algorithm algorithm,
        DecisionStatus status,
        std::chrono::nanoseconds latency);
    void record_redis_command(std::chrono::nanoseconds latency, bool ok);
    void record_error(ErrorCode code);
    void record_http_status(int status);
    void increment_active_requests() noexcept;
    void decrement_active_requests() noexcept;

    [[nodiscard]] std::string render_prometheus(
        std::string_view instance_id,
        const RedisStats& redis_stats) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace rl
