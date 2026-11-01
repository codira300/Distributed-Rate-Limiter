#include "config/service_config.hpp"

#include <cstdlib>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "ratelimiter/policy.hpp"

namespace rl {

namespace {

std::optional<std::string> env(std::string_view name) {
    const std::string key(name);
    if (const char* value = std::getenv(key.c_str()); value != nullptr) {
        return std::string(value);
    }
    return std::nullopt;
}

int parse_int(std::string_view value, std::string_view name) {
    try {
        std::size_t consumed = 0;
        const int parsed = std::stoi(std::string(value), &consumed, 10);
        if (consumed != value.size()) {
            throw std::invalid_argument("trailing characters");
        }
        return parsed;
    } catch (const std::exception&) {
        throw std::invalid_argument("invalid integer for " + std::string(name) + ": " + std::string(value));
    }
}

bool parse_bool(std::string_view value) {
    if (value == "1" || value == "true" || value == "yes" || value == "on") return true;
    if (value == "0" || value == "false" || value == "no" || value == "off") return false;
    throw std::invalid_argument("invalid boolean: " + std::string(value));
}

void apply_env(ServiceConfig& config) {
    if (const auto value = env("RL_INSTANCE_ID")) config.instance_id = *value;
    if (const auto value = env("RL_HTTP_HOST")) config.bind_host = *value;
    if (const auto value = env("RL_HTTP_PORT")) config.port = parse_int(*value, "RL_HTTP_PORT");
    if (const auto value = env("RL_POLICIES")) config.policy_file = *value;
    if (const auto value = env("RL_KEY_PREFIX")) config.key_prefix = *value;
    if (const auto value = env("RL_HTTP_THREADS")) config.http_threads = parse_int(*value, "RL_HTTP_THREADS");
    if (const auto value = env("RL_FAIL_OPEN")) config.fail_open = parse_bool(*value);
    if (const auto value = env("RL_IDLE_TTL_MS")) {
        config.idle_ttl = std::chrono::milliseconds(parse_int(*value, "RL_IDLE_TTL_MS"));
    }
    if (const auto value = env("RL_LOG_LEVEL")) config.log_level = parse_log_level(*value);

    if (const auto value = env("RL_REDIS_HOST")) config.redis.host = *value;
    if (const auto value = env("RL_REDIS_PORT")) config.redis.port = parse_int(*value, "RL_REDIS_PORT");
    if (const auto value = env("RL_REDIS_PASSWORD")) config.redis.password = *value;
    if (const auto value = env("RL_REDIS_DB")) config.redis.database = parse_int(*value, "RL_REDIS_DB");
    if (const auto value = env("RL_REDIS_POOL_SIZE")) {
        const int parsed = parse_int(*value, "RL_REDIS_POOL_SIZE");
        if (parsed <= 0) throw std::invalid_argument("RL_REDIS_POOL_SIZE must be positive");
        config.redis.pool_size = static_cast<std::size_t>(parsed);
    }
    if (const auto value = env("RL_REDIS_CONNECT_TIMEOUT_MS")) {
        config.redis.connect_timeout = std::chrono::milliseconds(
            parse_int(*value, "RL_REDIS_CONNECT_TIMEOUT_MS"));
    }
    if (const auto value = env("RL_REDIS_COMMAND_TIMEOUT_MS")) {
        config.redis.command_timeout = std::chrono::milliseconds(
            parse_int(*value, "RL_REDIS_COMMAND_TIMEOUT_MS"));
    }
    if (const auto value = env("RL_REDIS_ACQUIRE_TIMEOUT_MS")) {
        config.redis.acquire_timeout = std::chrono::milliseconds(
            parse_int(*value, "RL_REDIS_ACQUIRE_TIMEOUT_MS"));
    }
}

void apply_args(ServiceConfig& config, int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg(argv[i]);
        auto next = [&](std::string_view name) -> std::string {
            if (i + 1 >= argc) {
                throw std::invalid_argument("missing value for " + std::string(name));
            }
            return std::string(argv[++i]);
        };

        if (arg == "--instance-id") config.instance_id = next(arg);
        else if (arg == "--bind") config.bind_host = next(arg);
        else if (arg == "--port") config.port = parse_int(next(arg), arg);
        else if (arg == "--policies") config.policy_file = next(arg);
        else if (arg == "--key-prefix") config.key_prefix = next(arg);
        else if (arg == "--http-threads") config.http_threads = parse_int(next(arg), arg);
        else if (arg == "--fail-open") config.fail_open = true;
        else if (arg == "--fail-closed") config.fail_open = false;
        else if (arg == "--idle-ttl-ms") {
            config.idle_ttl = std::chrono::milliseconds(parse_int(next(arg), arg));
        } else if (arg == "--log-level") config.log_level = parse_log_level(next(arg));
        else if (arg == "--redis-host") config.redis.host = next(arg);
        else if (arg == "--redis-port") config.redis.port = parse_int(next(arg), arg);
        else if (arg == "--redis-password") config.redis.password = next(arg);
        else if (arg == "--redis-db") config.redis.database = parse_int(next(arg), arg);
        else if (arg == "--redis-pool-size") {
            const int parsed = parse_int(next(arg), arg);
            if (parsed <= 0) throw std::invalid_argument("--redis-pool-size must be positive");
            config.redis.pool_size = static_cast<std::size_t>(parsed);
        } else if (arg == "--redis-connect-timeout-ms") {
            config.redis.connect_timeout = std::chrono::milliseconds(parse_int(next(arg), arg));
        } else if (arg == "--redis-command-timeout-ms") {
            config.redis.command_timeout = std::chrono::milliseconds(parse_int(next(arg), arg));
        } else if (arg == "--redis-acquire-timeout-ms") {
            config.redis.acquire_timeout = std::chrono::milliseconds(parse_int(next(arg), arg));
        } else {
            throw std::invalid_argument("unknown argument: " + std::string(arg));
        }
    }
}

void validate(ServiceConfig& config) {
    if (config.port <= 0 || config.port > 65535) {
        throw std::invalid_argument("HTTP port must be between 1 and 65535");
    }
    if (config.bind_host.empty()) {
        throw std::invalid_argument("bind host must not be empty");
    }
    if (config.http_threads < 0 || config.http_threads > 1024) {
        throw std::invalid_argument("http threads must be between 0 and 1024");
    }
    if (config.idle_ttl.count() <= 0) {
        throw std::invalid_argument("idle TTL must be positive");
    }
    if (const auto error = validate_client_id(config.instance_id)) {
        throw std::invalid_argument("invalid instance id: " + error->message);
    }
}

} // namespace

ServiceConfig load_service_config(int argc, char** argv) {
    ServiceConfig config;
    apply_env(config);
    apply_args(config, argc, argv);
    validate(config);
    return config;
}

} // namespace rl
