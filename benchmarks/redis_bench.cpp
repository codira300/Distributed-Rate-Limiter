#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <iterator>
#include <memory>
#include <numeric>
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
    std::string algorithm{"tb"};
    int servers{1};
    int threads_per_server{1};
    int duration_seconds{5};
    std::uint64_t capacity{1'000'000};
    std::uint64_t limit{1'000'000};
    std::int64_t window_ms{60'000};
    std::string key_prefix;
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
    options.key_prefix = "rl:bench:" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());

    if (const auto value = environment("RL_TEST_REDIS_HOST")) options.redis_host = *value;
    else if (const auto fallback = environment("RL_REDIS_HOST")) options.redis_host = *fallback;
    if (const auto value = environment("RL_TEST_REDIS_PORT")) options.redis_port = parse_int(*value, "RL_TEST_REDIS_PORT");
    else if (const auto fallback = environment("RL_REDIS_PORT")) options.redis_port = parse_int(*fallback, "RL_REDIS_PORT");
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
        else if (argument == "--algorithm") options.algorithm = next(argument);
        else if (argument == "--servers") options.servers = parse_int(next(argument), argument);
        else if (argument == "--threads-per-server") {
            options.threads_per_server = parse_int(next(argument), argument);
        } else if (argument == "--duration") {
            options.duration_seconds = parse_int(next(argument), argument);
        } else if (argument == "--capacity") {
            options.capacity = static_cast<std::uint64_t>(parse_int(next(argument), argument));
        } else if (argument == "--limit") {
            options.limit = static_cast<std::uint64_t>(parse_int(next(argument), argument));
        } else if (argument == "--window-ms") {
            options.window_ms = static_cast<std::int64_t>(parse_int(next(argument), argument));
        } else if (argument == "--key-prefix") {
            options.key_prefix = next(argument);
        } else {
            throw std::invalid_argument("unknown argument: " + std::string(argument));
        }
    }

    if (options.algorithm != "tb" && options.algorithm != "sw") {
        throw std::invalid_argument("algorithm must be tb or sw");
    }
    if (options.redis_port <= 0 || options.redis_port > 65535) {
        throw std::invalid_argument("redis port must be between 1 and 65535");
    }
    if (options.servers <= 0 || options.servers > 16) {
        throw std::invalid_argument("servers must be between 1 and 16");
    }
    if (options.threads_per_server <= 0 || options.threads_per_server > 64) {
        throw std::invalid_argument("threads-per-server must be between 1 and 64");
    }
    if (options.duration_seconds <= 0) {
        throw std::invalid_argument("duration must be positive");
    }
    return options;
}

rl::Policy make_policy(const Options& options) {
    rl::Policy policy;
    if (options.algorithm == "tb") {
        policy.id = "bench-token-bucket";
        policy.algorithm = rl::Algorithm::token_bucket;
        policy.parameters = rl::TokenBucketPolicy{
            options.capacity,
            1'000'000'000,
            1,
            1,
        };
    } else {
        policy.id = "bench-sliding-window";
        policy.algorithm = rl::Algorithm::sliding_window;
        policy.parameters = rl::SlidingWindowPolicy{
            options.limit,
            std::chrono::milliseconds(options.window_ms),
            1,
            1,
        };
    }
    return policy;
}

double percentile(const std::vector<double>& sorted, double quantile) {
    if (sorted.empty()) {
        return 0.0;
    }
    const double position = quantile * static_cast<double>(sorted.size() - 1);
    const auto index = static_cast<std::size_t>(std::ceil(position));
    return sorted[std::min(index, sorted.size() - 1)];
}

struct Counters {
    std::atomic<std::uint64_t> allowed{0};
    std::atomic<std::uint64_t> denied{0};
    std::atomic<std::uint64_t> errors{0};
};

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

        const rl::Policy policy = make_policy(options);
        std::vector<std::unique_ptr<rl::RedisClient>> clients;
        std::vector<std::unique_ptr<rl::RateLimiter>> limiters;
        for (int i = 0; i < options.servers; ++i) {
            auto client = std::make_unique<rl::RedisClient>(redis_config);
            if (!client->ping()) {
                std::cerr << "Redis is required for the benchmark\n";
                return 2;
            }
            if (options.algorithm == "tb") {
                limiters.push_back(std::make_unique<rl::TokenBucketLimiter>(
                    *client,
                    policy,
                    options.key_prefix,
                    60s));
            } else {
                limiters.push_back(std::make_unique<rl::SlidingWindowLimiter>(
                    *client,
                    policy,
                    options.key_prefix));
            }
            clients.push_back(std::move(client));
        }

        const int total_threads = options.servers * options.threads_per_server;
        std::vector<std::vector<double>> samples(static_cast<std::size_t>(total_threads));
        std::vector<Counters> counters(static_cast<std::size_t>(options.servers));
        std::atomic<bool> start{false};
        std::vector<std::thread> workers;
        workers.reserve(static_cast<std::size_t>(total_threads));

        for (int server_index = 0; server_index < options.servers; ++server_index) {
            for (int thread_index = 0; thread_index < options.threads_per_server; ++thread_index) {
                const int sample_index = server_index * options.threads_per_server + thread_index;
                workers.emplace_back([&, server_index, sample_index] {
                    auto& local_samples = samples[static_cast<std::size_t>(sample_index)];
                    local_samples.reserve(250'000);
                    auto& counter = counters[static_cast<std::size_t>(server_index)];
                    while (!start.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(options.duration_seconds);
                    while (std::chrono::steady_clock::now() < deadline) {
                        const auto begin = std::chrono::steady_clock::now();
                        const auto result = limiters[static_cast<std::size_t>(server_index)]->check(
                            rl::CheckRequest{"bench-client", 0});
                        const auto end = std::chrono::steady_clock::now();
                        local_samples.push_back(
                            std::chrono::duration<double, std::micro>(end - begin).count());
                        if (!result) {
                            counter.errors.fetch_add(1, std::memory_order_relaxed);
                        } else if (result.value().allowed) {
                            counter.allowed.fetch_add(1, std::memory_order_relaxed);
                        } else {
                            counter.denied.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                });
            }
        }

        const auto test_start = std::chrono::steady_clock::now();
        start.store(true, std::memory_order_release);
        for (auto& worker : workers) {
            worker.join();
        }
        const auto test_end = std::chrono::steady_clock::now();
        const double elapsed = std::chrono::duration<double>(test_end - test_start).count();

        std::vector<double> all_samples;
        for (auto& local : samples) {
            all_samples.insert(
                all_samples.end(),
                std::make_move_iterator(local.begin()),
                std::make_move_iterator(local.end()));
        }
        std::sort(all_samples.begin(), all_samples.end());

        std::uint64_t total_allowed = 0;
        std::uint64_t total_denied = 0;
        std::uint64_t total_errors = 0;
        for (const auto& counter : counters) {
            total_allowed += counter.allowed.load();
            total_denied += counter.denied.load();
            total_errors += counter.errors.load();
        }
        const std::uint64_t total = total_allowed + total_denied + total_errors;

        std::cout << "algorithm=" << options.algorithm
                  << " servers=" << options.servers
                  << " threads_per_server=" << options.threads_per_server
                  << " duration_s=" << elapsed << '\n';
        std::cout << "checks=" << total
                  << " allowed=" << total_allowed
                  << " denied=" << total_denied
                  << " errors=" << total_errors
                  << " throughput_checks_per_s=" << (elapsed > 0.0 ? static_cast<double>(total) / elapsed : 0.0)
                  << '\n';
        if (!all_samples.empty()) {
            const double sum = std::accumulate(all_samples.begin(), all_samples.end(), 0.0);
            std::cout << "latency_us avg=" << (sum / static_cast<double>(all_samples.size()))
                      << " p50=" << percentile(all_samples, 0.50)
                      << " p95=" << percentile(all_samples, 0.95)
                      << " p99=" << percentile(all_samples, 0.99)
                      << " max=" << all_samples.back() << '\n';
        }

        for (std::size_t i = 0; i < clients.size(); ++i) {
            const auto stats = clients[i]->stats();
            std::cout << "server_" << (i + 1) << "_redis"
                      << " commands=" << stats.commands
                      << " command_errors=" << stats.command_errors
                      << " connections_created=" << stats.connections_created
                      << " noscript_recoveries=" << stats.noscript_recoveries
                      << " pool_acquire_timeouts=" << stats.pool_acquire_timeouts
                      << '\n';
        }

        return total_errors == 0 ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << "benchmark failure: " << error.what() << '\n';
        return 1;
    }
}
