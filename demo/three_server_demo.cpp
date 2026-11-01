#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "ratelimiter/rate_limiter.hpp"
#include "ratelimiter/redis_client.hpp"
#include "ratelimiter/sliding_window.hpp"
#include "ratelimiter/token_bucket.hpp"

namespace {

using namespace std::chrono_literals;

struct Options {
    std::string redis_host{"127.0.0.1"};
    int redis_port{6379};
    std::optional<std::string> redis_password;
    std::string key_prefix;
    std::string client_id{"shared-demo-client"};
    std::uint64_t capacity{100};
    std::uint64_t sliding_limit{50};
    std::int64_t window_ms{60000};
    int threads_per_server{4};
    int requests_per_thread{20};
};

int parse_int(std::string_view value, std::string_view name) {
    try {
        std::size_t consumed = 0;
        const int parsed = std::stoi(std::string(value), &consumed, 10);
        if (consumed != value.size()) {
            throw std::invalid_argument("trailing characters");
        }
        return parsed;
    } catch (const std::exception&) {
        throw std::invalid_argument("invalid integer for " + std::string(name));
    }
}

std::optional<std::string> environment(const char* name) {
    if (const char* value = std::getenv(name); value != nullptr) {
        return std::string(value);
    }
    return std::nullopt;
}

Options parse_options(int argc, char** argv) {
    Options options;
    options.key_prefix = "rl:demo:" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());

    if (const auto value = environment("RL_TEST_REDIS_HOST")) options.redis_host = *value;
    else if (const auto fallback = environment("RL_REDIS_HOST")) options.redis_host = *fallback;
    if (const auto value = environment("RL_TEST_REDIS_PORT")) options.redis_port = std::stoi(*value);
    else if (const auto fallback = environment("RL_REDIS_PORT")) options.redis_port = std::stoi(*fallback);
    if (const auto value = environment("RL_TEST_REDIS_PASSWORD")) options.redis_password = *value;
    else if (const auto fallback = environment("RL_REDIS_PASSWORD")) options.redis_password = *fallback;

    for (int i = 1; i < argc; ++i) {
        const std::string_view argument(argv[i]);
        auto next = [&](std::string_view name) -> std::string {
            if (i + 1 >= argc) {
                throw std::invalid_argument("missing value for " + std::string(name));
            }
            return std::string(argv[++i]);
        };

        if (argument == "--redis-host") options.redis_host = next(argument);
        else if (argument == "--redis-port") options.redis_port = parse_int(next(argument), argument);
        else if (argument == "--redis-password") options.redis_password = next(argument);
        else if (argument == "--key-prefix") options.key_prefix = next(argument);
        else if (argument == "--client-id") options.client_id = next(argument);
        else if (argument == "--capacity") {
            options.capacity = static_cast<std::uint64_t>(parse_int(next(argument), argument));
        } else if (argument == "--limit") {
            options.sliding_limit = static_cast<std::uint64_t>(parse_int(next(argument), argument));
        } else if (argument == "--window-ms") {
            options.window_ms = static_cast<std::int64_t>(parse_int(next(argument), argument));
        } else if (argument == "--threads-per-server") {
            options.threads_per_server = parse_int(next(argument), argument);
        } else if (argument == "--requests-per-thread") {
            options.requests_per_thread = parse_int(next(argument), argument);
        } else {
            throw std::invalid_argument("unknown argument: " + std::string(argument));
        }
    }

    if (options.redis_port <= 0 || options.redis_port > 65535) {
        throw std::invalid_argument("redis port must be between 1 and 65535");
    }
    if (options.capacity == 0 || options.sliding_limit == 0) {
        throw std::invalid_argument("capacity and limit must be positive");
    }
    if (options.window_ms <= 0) {
        throw std::invalid_argument("window must be positive");
    }
    if (options.threads_per_server <= 0 || options.threads_per_server > 256) {
        throw std::invalid_argument("threads-per-server must be between 1 and 256");
    }
    if (options.requests_per_thread <= 0) {
        throw std::invalid_argument("requests-per-thread must be positive");
    }
    return options;
}

rl::Policy token_bucket_policy(const Options& options) {
    rl::Policy policy;
    policy.id = "demo-token-bucket";
    policy.algorithm = rl::Algorithm::token_bucket;
    policy.parameters = rl::TokenBucketPolicy{
        options.capacity,
        1,
        1,
        1,
    };
    return policy;
}

rl::Policy sliding_window_policy(const Options& options) {
    rl::Policy policy;
    policy.id = "demo-sliding-window";
    policy.algorithm = rl::Algorithm::sliding_window;
    policy.parameters = rl::SlidingWindowPolicy{
        options.sliding_limit,
        std::chrono::milliseconds(options.window_ms),
        1,
        1,
    };
    return policy;
}

struct Server {
    std::string id;
    std::unique_ptr<rl::RedisClient> redis;
    std::unique_ptr<rl::RateLimiter> token_bucket;
    std::unique_ptr<rl::RateLimiter> sliding_window;
};

struct Tally {
    std::atomic<int> allowed{0};
    std::atomic<int> denied{0};
    std::atomic<int> errors{0};
};

using LimiterMember = std::unique_ptr<rl::RateLimiter> Server::*;

void run_checks(
    std::vector<std::unique_ptr<Server>>& servers,
    const Options& options,
    std::vector<Tally>& tallies,
    LimiterMember limiter_member) {
    const int total_threads = static_cast<int>(servers.size()) * options.threads_per_server;
    std::barrier start(total_threads);
    std::vector<std::thread> workers;
    workers.reserve(static_cast<std::size_t>(total_threads));

    for (std::size_t server_index = 0; server_index < servers.size(); ++server_index) {
        for (int thread_index = 0; thread_index < options.threads_per_server; ++thread_index) {
            workers.emplace_back([&, server_index] {
                start.arrive_and_wait();
                auto& tally = tallies[server_index];
                const auto& limiter = servers[server_index].get()->*limiter_member;
                for (int request = 0; request < options.requests_per_thread; ++request) {
                    const auto result = limiter->check(rl::CheckRequest{options.client_id, 0});
                    if (!result) {
                        tally.errors.fetch_add(1, std::memory_order_relaxed);
                    } else if (result.value().allowed) {
                        tally.allowed.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        tally.denied.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }
    }
    for (auto& worker : workers) {
        worker.join();
    }
}

std::int64_t probe_pttl(rl::RedisClient& redis, const std::string& key) {
    const auto reply = redis.eval_script(
        "demo_pttl",
        "return {redis.call('PTTL', KEYS[1])}",
        {key},
        {});
    if (!reply || reply.value().integers.empty()) {
        return -2;
    }
    return reply.value().integers[0];
}

} // namespace

int main(int argc, char** argv) {
    try {
        const Options options = parse_options(argc, argv);

        rl::RedisConfig redis_config;
        redis_config.host = options.redis_host;
        redis_config.port = options.redis_port;
        redis_config.password = options.redis_password;
        redis_config.pool_size = static_cast<std::size_t>(options.threads_per_server);
        redis_config.connect_timeout = 2000ms;
        redis_config.command_timeout = 3000ms;
        redis_config.acquire_timeout = 5000ms;

        const rl::Policy tb_policy = token_bucket_policy(options);
        const rl::Policy sw_policy = sliding_window_policy(options);

        std::vector<std::unique_ptr<Server>> servers;
        for (int i = 0; i < 3; ++i) {
            auto server = std::make_unique<Server>();
            server->id = "app-" + std::to_string(i + 1);
            server->redis = std::make_unique<rl::RedisClient>(redis_config);
            if (!server->redis->ping()) {
                std::cerr << "Redis is required for the demo\n";
                return 2;
            }
            server->token_bucket = std::make_unique<rl::TokenBucketLimiter>(
                *server->redis,
                tb_policy,
                options.key_prefix,
                60s);
            server->sliding_window = std::make_unique<rl::SlidingWindowLimiter>(
                *server->redis,
                sw_policy,
                options.key_prefix);
            servers.push_back(std::move(server));
        }

        std::vector<Tally> tallies(servers.size());
        run_checks(servers, options, tallies, &Server::token_bucket);

        const int tb_total = static_cast<int>(servers.size()) *
            options.threads_per_server * options.requests_per_thread;
        int tb_allowed = 0;
        int tb_denied = 0;
        int tb_errors = 0;
        std::cout << "token bucket: capacity=" << options.capacity
                  << " servers=3 threads_per_server=" << options.threads_per_server
                  << " requests=" << tb_total << '\n';
        for (std::size_t i = 0; i < servers.size(); ++i) {
            const int allowed = tallies[i].allowed.load();
            const int denied = tallies[i].denied.load();
            const int errors = tallies[i].errors.load();
            tb_allowed += allowed;
            tb_denied += denied;
            tb_errors += errors;
            std::cout << "  " << servers[i]->id << ": allowed=" << allowed
                      << " denied=" << denied << " errors=" << errors << '\n';
        }
        std::cout << "  global: allowed=" << tb_allowed << " denied=" << tb_denied
                  << " errors=" << tb_errors << '\n';

        bool ok = tb_errors == 0;
        ok = ok && tb_allowed == static_cast<int>(options.capacity);
        ok = ok && tb_denied == tb_total - static_cast<int>(options.capacity);

        const std::string tb_key = options.key_prefix + ":tb:{" +
            tb_policy.id + ":" + options.client_id + "}";
        const std::int64_t tb_ttl = probe_pttl(*servers[0]->redis, tb_key);
        std::cout << "  redis key=" << tb_key << " pttl_ms=" << tb_ttl << '\n';
        ok = ok && tb_ttl > 0;

        for (auto& tally : tallies) {
            tally.allowed.store(0);
            tally.denied.store(0);
            tally.errors.store(0);
        }
        run_checks(servers, options, tallies, &Server::sliding_window);

        const int sw_total = static_cast<int>(servers.size()) *
            options.threads_per_server * options.requests_per_thread;
        int sw_allowed = 0;
        int sw_denied = 0;
        int sw_errors = 0;
        std::cout << "sliding window: limit=" << options.sliding_limit
                  << " window_ms=" << options.window_ms
                  << " servers=3 threads_per_server=" << options.threads_per_server
                  << " requests=" << sw_total << '\n';
        for (std::size_t i = 0; i < servers.size(); ++i) {
            const int allowed = tallies[i].allowed.load();
            const int denied = tallies[i].denied.load();
            const int errors = tallies[i].errors.load();
            sw_allowed += allowed;
            sw_denied += denied;
            sw_errors += errors;
            std::cout << "  " << servers[i]->id << ": allowed=" << allowed
                      << " denied=" << denied << " errors=" << errors << '\n';
        }
        std::cout << "  global: allowed=" << sw_allowed << " denied=" << sw_denied
                  << " errors=" << sw_errors << '\n';

        ok = ok && sw_errors == 0;
        ok = ok && sw_allowed == static_cast<int>(options.sliding_limit);
        ok = ok && sw_denied == sw_total - static_cast<int>(options.sliding_limit);

        const std::string sw_key = options.key_prefix + ":sw:{" +
            sw_policy.id + ":" + options.client_id + "}:log";
        const std::int64_t sw_ttl = probe_pttl(*servers[0]->redis, sw_key);
        std::cout << "  redis key=" << sw_key << " pttl_ms=" << sw_ttl << '\n';
        ok = ok && sw_ttl > 0;

        if (!ok) {
            std::cerr << "demo failed: the global limit was not enforced exactly\n";
            return 1;
        }
        std::cout << "demo passed: three independent servers enforced one global limit\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "demo failure: " << error.what() << '\n';
        return 1;
    }
}
