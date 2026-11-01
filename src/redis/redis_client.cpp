#include "ratelimiter/redis_client.hpp"

#include <hiredis/hiredis.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace rl {

namespace {

struct ReplyDeleter {
    void operator()(redisReply* reply) const noexcept {
        if (reply != nullptr) {
            freeReplyObject(reply);
        }
    }
};

using ReplyPtr = std::unique_ptr<redisReply, ReplyDeleter>;

timeval to_timeval(std::chrono::milliseconds value) {
    timeval tv{};
    tv.tv_sec = static_cast<long>(value.count() / 1000);
    tv.tv_usec = static_cast<long>((value.count() % 1000) * 1000);
    return tv;
}

Error context_error(redisContext* context, std::string_view operation) {
    const bool timeout = context->err == REDIS_ERR_TIMEOUT;
    std::string message(operation);
    message += ": ";
    message += context->errstr[0] != '\0' ? context->errstr : "Redis connection error";
    return Error{
        timeout ? ErrorCode::redis_timeout : ErrorCode::redis_unavailable,
        std::move(message),
        true};
}

bool run_command(
    redisContext* context,
    const std::vector<std::string>& parts,
    ReplyPtr& reply_out,
    Error& error_out,
    bool& connection_broken) {
    connection_broken = false;
    std::vector<const char*> argv;
    argv.reserve(parts.size());
    for (const auto& part : parts) {
        argv.push_back(part.c_str());
    }

    auto* raw = static_cast<redisReply*>(
        redisCommandArgv(context, static_cast<int>(argv.size()), argv.data(), nullptr));
    if (raw == nullptr) {
        connection_broken = true;
        error_out = context_error(context, "Redis command failed");
        return false;
    }

    reply_out.reset(raw);
    if (reply_out->type == REDIS_REPLY_ERROR) {
        const std::string message = reply_out->str != nullptr ? reply_out->str : "unknown Redis error";
        error_out = Error{ErrorCode::redis_protocol, message, false};
        return false;
    }
    return true;
}

bool is_noscript(const Error& error) {
    return error.code == ErrorCode::redis_protocol &&
           error.message.rfind("NOSCRIPT", 0) == 0;
}

std::optional<std::int64_t> reply_integer(const redisReply* reply) {
    if (reply == nullptr) {
        return std::nullopt;
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    }
    if (reply->type == REDIS_REPLY_STRING && reply->str != nullptr) {
        char* end = nullptr;
        const long long value = std::strtoll(reply->str, &end, 10);
        if (end != reply->str && *end == '\0') {
            return static_cast<std::int64_t>(value);
        }
    }
    return std::nullopt;
}

struct RedisConnection {
    explicit RedisConnection(redisContext* context) : ctx(context) {}
    ~RedisConnection() {
        if (ctx != nullptr) {
            redisFree(ctx);
        }
    }

    RedisConnection(const RedisConnection&) = delete;
    RedisConnection& operator=(const RedisConnection&) = delete;

    redisContext* ctx{nullptr};
};

} // namespace

struct RedisClient::Impl {
    explicit Impl(RedisConfig config_value) : config(std::move(config_value)) {
        if (config.host.empty()) {
            throw std::invalid_argument("Redis host must not be empty");
        }
        if (config.port <= 0 || config.port > 65535) {
            throw std::invalid_argument("Redis port must be between 1 and 65535");
        }
        if (config.pool_size == 0 || config.pool_size > 1024) {
            throw std::invalid_argument("Redis pool size must be between 1 and 1024");
        }
        if (config.database < 0) {
            throw std::invalid_argument("Redis database must be non-negative");
        }
        if (config.connect_timeout.count() <= 0 ||
            config.command_timeout.count() <= 0 ||
            config.acquire_timeout.count() <= 0) {
            throw std::invalid_argument("Redis timeouts must be positive");
        }
    }

    struct Lease {
        Impl& owner;
        std::unique_ptr<RedisConnection> connection;
        bool healthy{true};

        Lease(Impl& owner_ref, std::unique_ptr<RedisConnection> connection_value)
            : owner(owner_ref), connection(std::move(connection_value)) {}

        ~Lease() { owner.release(std::move(connection), healthy); }

        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;

        void mark_unhealthy() noexcept { healthy = false; }
        [[nodiscard]] redisContext* context() const noexcept { return connection->ctx; }
    };

    struct ConnectionGuard {
        Impl& owner;
        std::unique_ptr<RedisConnection>& connection;
        bool active{true};

        ~ConnectionGuard() {
            if (active) {
                owner.release(std::move(connection), false);
            }
        }

        void disarm() noexcept { active = false; }
    };

    RedisConfig config;

    mutable std::mutex pool_mutex;
    std::condition_variable pool_cv;
    std::vector<std::unique_ptr<RedisConnection>> idle;
    std::size_t total{0};
    bool shutting_down{false};
    std::chrono::steady_clock::time_point unavailable_until{};

    struct StringHash {
        using is_transparent = void;

        [[nodiscard]] std::size_t operator()(std::string_view value) const noexcept {
            return std::hash<std::string_view>{}(value);
        }
        [[nodiscard]] std::size_t operator()(const std::string& value) const noexcept {
            return std::hash<std::string_view>{}(value);
        }
    };

    std::mutex script_mutex;
    std::unordered_map<std::string, std::string, StringHash, std::equal_to<>> script_shas;

    std::atomic<std::uint64_t> connections_created{0};
    std::atomic<std::uint64_t> connection_failures{0};
    std::atomic<std::uint64_t> commands{0};
    std::atomic<std::uint64_t> command_errors{0};
    std::atomic<std::uint64_t> noscript_recoveries{0};
    std::atomic<std::uint64_t> pool_acquire_timeouts{0};

    ~Impl() { shutdown(); }

    void shutdown() noexcept {
        std::lock_guard lock(pool_mutex);
        shutting_down = true;
        total = total >= idle.size() ? total - idle.size() : 0;
        idle.clear();
        pool_cv.notify_all();
    }

    void mark_unavailable() noexcept {
        std::lock_guard lock(pool_mutex);
        unavailable_until = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
    }

    [[nodiscard]] Result<std::unique_ptr<RedisConnection>> create_connection() {
        const timeval connect_timeout = to_timeval(config.connect_timeout);
        redisContext* context = redisConnectWithTimeout(config.host.c_str(), config.port, connect_timeout);
        if (context == nullptr) {
            return Error{ErrorCode::redis_unavailable, "redisConnectWithTimeout allocated no context", true};
        }
        std::unique_ptr<RedisConnection> connection = std::make_unique<RedisConnection>(context);
        if (context->err != 0) {
            return context_error(context, "Redis connection failed");
        }

        const timeval command_timeout = to_timeval(config.command_timeout);
        if (redisSetTimeout(context, command_timeout) != REDIS_OK) {
            return context_error(context, "failed to configure Redis socket timeout");
        }
        if (redisEnableKeepAlive(context) != REDIS_OK) {
            return context_error(context, "failed to enable Redis TCP keepalive");
        }

        auto execute_setup = [&](const std::vector<std::string>& parts) -> Error {
            ReplyPtr reply;
            Error error{};
            bool broken = false;
            commands.fetch_add(1, std::memory_order_relaxed);
            if (!run_command(context, parts, reply, error, broken)) {
                command_errors.fetch_add(1, std::memory_order_relaxed);
                return error;
            }
            return Error{ErrorCode::internal, "", false};
        };

        if (config.password.has_value()) {
            const Error error = execute_setup({"AUTH", *config.password});
            if (error.code != ErrorCode::internal) {
                return error;
            }
        }

        if (config.database != 0) {
            const Error error = execute_setup({"SELECT", std::to_string(config.database)});
            if (error.code != ErrorCode::internal) {
                return error;
            }
        }

        connections_created.fetch_add(1, std::memory_order_relaxed);
        return connection;
    }

    [[nodiscard]] Result<std::unique_ptr<Lease>> acquire() {
        std::unique_ptr<RedisConnection> connection;
        {
            std::unique_lock lock(pool_mutex);
            if (shutting_down) {
                return Error{ErrorCode::redis_unavailable, "Redis client is shutting down", true};
            }
            if (std::chrono::steady_clock::now() < unavailable_until) {
                return Error{ErrorCode::redis_unavailable, "Redis is in failure cooldown", true};
            }

            if (!idle.empty()) {
                connection = std::move(idle.back());
                idle.pop_back();
            } else if (total < config.pool_size) {
                ++total;
            } else {
                const bool available = pool_cv.wait_for(
                    lock,
                    config.acquire_timeout,
                    [this] { return !idle.empty() || shutting_down; });
                if (!available) {
                    pool_acquire_timeouts.fetch_add(1, std::memory_order_relaxed);
                    return Error{ErrorCode::pool_exhausted, "Redis connection pool acquire timed out", true};
                }
                if (shutting_down) {
                    return Error{ErrorCode::redis_unavailable, "Redis client is shutting down", true};
                }
                connection = std::move(idle.back());
                idle.pop_back();
            }
        }

        ConnectionGuard guard{*this, connection};
        if (!connection) {
            auto created = create_connection();
            if (!created) {
                {
                    std::lock_guard lock(pool_mutex);
                    unavailable_until = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
                }
                connection_failures.fetch_add(1, std::memory_order_relaxed);
                return created.error();
            }
            connection = std::move(created.value());
        }

        auto lease = std::make_unique<Lease>(*this, std::move(connection));
        guard.disarm();
        return lease;
    }

    void release(std::unique_ptr<RedisConnection> connection, bool healthy) noexcept {
        {
            std::lock_guard lock(pool_mutex);
            if (healthy && !shutting_down && connection) {
                idle.push_back(std::move(connection));
            } else {
                if (total > 0) {
                    --total;
                }
            }
        }
        pool_cv.notify_one();
    }

    [[nodiscard]] Result<std::string> load_script(Lease& lease, std::string_view body) {
        ReplyPtr reply;
        Error error{};
        bool broken = false;
        commands.fetch_add(1, std::memory_order_relaxed);
        if (!run_command(lease.context(), {"SCRIPT", "LOAD", std::string(body)}, reply, error, broken)) {
            command_errors.fetch_add(1, std::memory_order_relaxed);
            if (broken) {
                lease.mark_unhealthy();
                mark_unavailable();
            }
            return error;
        }
        if (reply->type != REDIS_REPLY_STRING || reply->str == nullptr) {
            return Error{ErrorCode::redis_protocol, "SCRIPT LOAD returned an unexpected reply", true};
        }
        return std::string(reply->str, static_cast<std::size_t>(reply->len));
    }

    [[nodiscard]] Result<ScriptReply> eval_script(
        std::string_view script_name,
        std::string_view script_body,
        const std::vector<std::string>& keys,
        const std::vector<std::string>& args) {
        auto lease_result = acquire();
        if (!lease_result) {
            return lease_result.error();
        }
        auto lease = std::move(lease_result.value());

        std::string sha;
        {
            std::lock_guard lock(script_mutex);
            const auto it = script_shas.find(script_name);
            if (it != script_shas.end()) {
                sha = it->second;
            }
        }

        if (sha.empty()) {
            auto loaded = load_script(*lease, script_body);
            if (!loaded) {
                return loaded.error();
            }
            sha = std::move(loaded.value());
            std::lock_guard lock(script_mutex);
            script_shas.insert_or_assign(std::string(script_name), sha);
        }

        auto make_eval_parts = [&](std::string_view command, std::string_view body_or_sha) {
            std::vector<std::string> parts;
            parts.reserve(3 + keys.size() + args.size());
            parts.emplace_back(command);
            parts.emplace_back(body_or_sha);
            parts.emplace_back(std::to_string(keys.size()));
            parts.insert(parts.end(), keys.begin(), keys.end());
            parts.insert(parts.end(), args.begin(), args.end());
            return parts;
        };

        ReplyPtr reply;
        Error error{};
        bool broken = false;
        commands.fetch_add(1, std::memory_order_relaxed);
        if (!run_command(
                lease->context(),
                make_eval_parts("EVALSHA", sha),
                reply,
                error,
                broken)) {
            command_errors.fetch_add(1, std::memory_order_relaxed);
            if (broken) {
                lease->mark_unhealthy();
                mark_unavailable();
                return error;
            }
            if (!is_noscript(error)) {
                return error;
            }

            noscript_recoveries.fetch_add(1, std::memory_order_relaxed);
            reply.reset();
            broken = false;
            commands.fetch_add(1, std::memory_order_relaxed);
            if (!run_command(
                    lease->context(),
                    make_eval_parts("EVAL", script_body),
                    reply,
                    error,
                    broken)) {
                command_errors.fetch_add(1, std::memory_order_relaxed);
                if (broken) {
                    lease->mark_unhealthy();
                    mark_unavailable();
                }
                return error;
            }

            auto reloaded = load_script(*lease, script_body);
            if (reloaded) {
                sha = std::move(reloaded.value());
                std::lock_guard lock(script_mutex);
                script_shas.insert_or_assign(std::string(script_name), sha);
            }
        }

        if (reply->type != REDIS_REPLY_ARRAY) {
            return Error{ErrorCode::redis_protocol, "rate limit script returned a non-array reply", true};
        }

        ScriptReply result;
        result.integers.reserve(reply->elements);
        for (std::size_t i = 0; i < reply->elements; ++i) {
            const auto value = reply_integer(reply->element[i]);
            if (!value.has_value()) {
                return Error{ErrorCode::redis_protocol, "rate limit script returned a non-integer element", true};
            }
            result.integers.push_back(*value);
        }
        return result;
    }

    [[nodiscard]] Result<bool> ping() {
        auto lease_result = acquire();
        if (!lease_result) {
            return lease_result.error();
        }
        auto lease = std::move(lease_result.value());

        ReplyPtr reply;
        Error error{};
        bool broken = false;
        commands.fetch_add(1, std::memory_order_relaxed);
        if (!run_command(lease->context(), {"PING"}, reply, error, broken)) {
            command_errors.fetch_add(1, std::memory_order_relaxed);
            if (broken) {
                lease->mark_unhealthy();
                mark_unavailable();
            }
            return error;
        }
        if (reply->type != REDIS_REPLY_STATUS || reply->str == nullptr ||
            std::string_view(reply->str, static_cast<std::size_t>(reply->len)) != "PONG") {
            return Error{ErrorCode::redis_protocol, "unexpected PING reply", true};
        }
        return true;
    }

    [[nodiscard]] RedisStats snapshot() const {
        RedisStats stats;
        stats.pool_size = config.pool_size;
        {
            std::lock_guard lock(pool_mutex);
            stats.idle = idle.size();
            stats.in_use = total >= idle.size() ? total - idle.size() : 0;
        }
        stats.connections_created = connections_created.load(std::memory_order_relaxed);
        stats.connection_failures = connection_failures.load(std::memory_order_relaxed);
        stats.commands = commands.load(std::memory_order_relaxed);
        stats.command_errors = command_errors.load(std::memory_order_relaxed);
        stats.noscript_recoveries = noscript_recoveries.load(std::memory_order_relaxed);
        stats.pool_acquire_timeouts = pool_acquire_timeouts.load(std::memory_order_relaxed);
        return stats;
    }
};

RedisClient::RedisClient(RedisConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

RedisClient::~RedisClient() = default;

RedisClient::RedisClient(RedisClient&& other) noexcept = default;
RedisClient& RedisClient::operator=(RedisClient&& other) noexcept = default;

Result<ScriptReply> RedisClient::eval_script(
    std::string_view script_name,
    std::string_view script_body,
    const std::vector<std::string>& keys,
    const std::vector<std::string>& args) {
    if (!impl_) {
        return Error{ErrorCode::internal, "Redis client has been moved from", false};
    }
    return impl_->eval_script(script_name, script_body, keys, args);
}

Result<bool> RedisClient::ping() {
    if (!impl_) {
        return Error{ErrorCode::internal, "Redis client has been moved from", false};
    }
    return impl_->ping();
}

RedisStats RedisClient::stats() const {
    return impl_ ? impl_->snapshot() : RedisStats{};
}

const RedisConfig& RedisClient::config() const noexcept {
    static const RedisConfig empty_config{};
    return impl_ ? impl_->config : empty_config;
}

} // namespace rl
