#pragma once

#include <chrono>
#include <string>

#include "ratelimiter/logger.hpp"
#include "ratelimiter/redis_client.hpp"

namespace rl {

struct ServiceConfig {
    std::string instance_id{"app-1"};
    std::string bind_host{"0.0.0.0"};
    int port{8081};
    std::string policy_file{"config/policies.json"};
    std::string key_prefix{"rl:v1"};
    int http_threads{0};
    bool fail_open{false};
    std::chrono::milliseconds idle_ttl{60000};
    LogLevel log_level{LogLevel::info};
    RedisConfig redis;
};

[[nodiscard]] ServiceConfig load_service_config(int argc, char** argv);

} // namespace rl
