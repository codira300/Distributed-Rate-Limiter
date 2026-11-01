#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "config/policy_registry.hpp"
#include "ratelimiter/rate_limiter.hpp"
#include "ratelimiter/redis_client.hpp"
#include "ratelimiter/sliding_window.hpp"
#include "ratelimiter/token_bucket.hpp"

namespace {

using namespace rl;
using namespace std::chrono_literals;

int failures = 0;

void check(bool condition, const std::string& message) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

template <typename T>
T require(const Result<T>& result, const std::string& context) {
    if (!result) {
        throw std::runtime_error(context + ": " + result.error().message);
    }
    return result.value();
}

Policy token_bucket_policy(
    std::string id,
    std::uint64_t capacity,
    std::uint64_t refill_microtokens,
    std::uint32_t default_cost = 1,
    std::uint32_t max_cost = 1) {
    Policy policy;
    policy.id = std::move(id);
    policy.algorithm = Algorithm::token_bucket;
    policy.parameters = TokenBucketPolicy{capacity, refill_microtokens, default_cost, max_cost};
    return policy;
}

Policy sliding_window_policy(
    std::string id,
    std::uint64_t limit,
    std::chrono::milliseconds window,
    std::uint32_t default_cost = 1,
    std::uint32_t max_cost = 1) {
    Policy policy;
    policy.id = std::move(id);
    policy.algorithm = Algorithm::sliding_window;
    policy.parameters = SlidingWindowPolicy{limit, window, default_cost, max_cost};
    return policy;
}

std::string unique_prefix() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::random_device device;
    const auto random = static_cast<unsigned long long>(device());
    return "rl:test:" + std::to_string(now) + ":" + std::to_string(random);
}

RedisConfig redis_config_from_environment(std::size_t pool_size) {
    RedisConfig config;
    if (const char* host = std::getenv("RL_TEST_REDIS_HOST"); host != nullptr) {
        config.host = host;
    }
    if (const char* port = std::getenv("RL_TEST_REDIS_PORT"); port != nullptr) {
        config.port = std::stoi(port);
    }
    if (const char* password = std::getenv("RL_TEST_REDIS_PASSWORD"); password != nullptr) {
        config.password = std::string(password);
    }
    config.pool_size = pool_size;
    config.connect_timeout = 2000ms;
    config.command_timeout = 3000ms;
    config.acquire_timeout = 5000ms;
    return config;
}

bool is_code(const Result<Decision>& result, ErrorCode code) {
    return !result && result.error().code == code;
}

void test_token_bucket_burst(const std::string& prefix, RedisClient& redis) {
    TokenBucketLimiter limiter(
        redis,
        token_bucket_policy("tb-burst", 3, 1),
        prefix,
        60s);

    for (int i = 0; i < 3; ++i) {
        const auto& decision = require(
            limiter.check(CheckRequest{"burst-client", 0}),
            "token bucket burst allow");
        check(decision.allowed, "token bucket request within capacity must be allowed");
        check(decision.status == DecisionStatus::allowed, "token bucket allow status");
        check(decision.remaining == static_cast<std::uint64_t>(2 - i), "token bucket remaining");
        check(decision.limit == 3, "token bucket limit");
    }

    const auto& denied = require(
        limiter.check(CheckRequest{"burst-client", 0}),
        "token bucket denied request");
    check(!denied.allowed, "token bucket must deny beyond capacity");
    check(denied.status == DecisionStatus::rate_limited, "token bucket denied status");
    check(denied.retry_after.count() > 0, "token bucket retry_after must be positive");
    check(denied.remaining == 0, "token bucket remaining must be zero");
}

void test_token_bucket_cost(const std::string& prefix, RedisClient& redis) {
    TokenBucketLimiter limiter(
        redis,
        token_bucket_policy("tb-cost", 10, 1, 1, 10),
        prefix,
        60s);

    const auto& first = require(
        limiter.check(CheckRequest{"cost-client", 7}),
        "token bucket cost 7");
    check(first.allowed && first.remaining == 3, "token bucket cost must reserve seven tokens");

    const auto& second = require(
        limiter.check(CheckRequest{"cost-client", 4}),
        "token bucket cost 4 denied");
    check(!second.allowed, "token bucket must deny when cost exceeds remaining tokens");

    const auto& third = require(
        limiter.check(CheckRequest{"cost-client", 3}),
        "token bucket cost 3");
    check(third.allowed && third.remaining == 0, "token bucket must allow exact remaining cost");

    check(
        is_code(limiter.check(CheckRequest{"cost-client", 11}), ErrorCode::cost_exceeds_limit),
        "token bucket cost above max_cost must be rejected");
}

void test_token_bucket_refill(const std::string& prefix, RedisClient& redis) {
    TokenBucketLimiter limiter(
        redis,
        token_bucket_policy("tb-refill", 2, 1'000'000),
        prefix,
        60s);

    check(require(limiter.check(CheckRequest{"refill-client", 0}), "refill initial 1").allowed, "refill initial 1");
    check(require(limiter.check(CheckRequest{"refill-client", 0}), "refill initial 2").allowed, "refill initial 2");
    check(!require(limiter.check(CheckRequest{"refill-client", 0}), "refill denied").allowed, "refill must initially deny");

    std::this_thread::sleep_for(1200ms);

    const auto& refilled = require(
        limiter.check(CheckRequest{"refill-client", 0}),
        "refill after sleep");
    check(refilled.allowed, "token bucket must refill over time");
    check(refilled.remaining == 0, "token bucket refill should leave zero tokens");
}

void test_token_bucket_ttl(const std::string& prefix, RedisClient& redis) {
    TokenBucketLimiter limiter(
        redis,
        token_bucket_policy("tb-ttl", 1, 1'000'000),
        prefix,
        60s);

    require(limiter.check(CheckRequest{"ttl-client", 0}), "ttl consume token");
    const std::string key = prefix + ":tb:{tb-ttl:ttl-client}";
    const auto reply = redis.eval_script(
        "ttl_probe",
        "return {redis.call('PTTL', KEYS[1])}",
        {key},
        {});
    const auto& values = require(reply, "ttl probe");
    check(!values.integers.empty(), "ttl probe must return a value");
    if (!values.integers.empty()) {
        check(values.integers[0] > 0, "token bucket key must have a positive TTL");
        check(values.integers[0] <= 3'000, "token bucket TTL must be bounded");
    }
}

void test_sliding_window_burst(const std::string& prefix, RedisClient& redis) {
    SlidingWindowLimiter limiter(
        redis,
        sliding_window_policy("sw-burst", 3, 600ms),
        prefix);

    for (int i = 0; i < 3; ++i) {
        const auto& decision = require(
            limiter.check(CheckRequest{"window-client", 0}),
            "sliding window allow");
        check(decision.allowed, "sliding window request within limit must be allowed");
        check(decision.remaining == static_cast<std::uint64_t>(2 - i), "sliding window remaining");
    }

    const auto& denied = require(
        limiter.check(CheckRequest{"window-client", 0}),
        "sliding window deny");
    check(!denied.allowed, "sliding window must deny beyond the limit");
    check(denied.retry_after.count() > 0 && denied.retry_after <= 650ms,
          "sliding window retry_after must be within the window");

    std::this_thread::sleep_for(750ms);
    const auto& after_window = require(
        limiter.check(CheckRequest{"window-client", 0}),
        "sliding window after window");
    check(after_window.allowed, "sliding window must allow after the window elapses");
}

void test_sliding_window_cost(const std::string& prefix, RedisClient& redis) {
    SlidingWindowLimiter limiter(
        redis,
        sliding_window_policy("sw-cost", 5, 3s, 1, 5),
        prefix);

    const auto& first = require(
        limiter.check(CheckRequest{"window-cost-client", 3}),
        "sliding window cost 3");
    check(first.allowed && first.remaining == 2, "sliding window must reserve weighted cost");

    const auto& second = require(
        limiter.check(CheckRequest{"window-cost-client", 3}),
        "sliding window cost 3 denied");
    check(!second.allowed, "sliding window must deny weighted cost over remaining");

    const auto& third = require(
        limiter.check(CheckRequest{"window-cost-client", 2}),
        "sliding window cost 2");
    check(third.allowed && third.remaining == 0, "sliding window must allow exact remaining cost");

    check(
        is_code(limiter.check(CheckRequest{"window-cost-client", 6}), ErrorCode::cost_exceeds_limit),
        "sliding window cost above max_cost must be rejected");
}

void test_sliding_window_concurrency(const std::string& prefix, RedisClient& redis) {
    SlidingWindowLimiter limiter(
        redis,
        sliding_window_policy("sw-concurrency", 20, 5s),
        prefix);

    constexpr int threads = 40;
    std::barrier start(threads);
    std::atomic<int> allowed{0};
    std::atomic<int> denied{0};
    std::atomic<int> errors{0};
    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back([&] {
            start.arrive_and_wait();
            const auto result = limiter.check(CheckRequest{"concurrent-window-client", 0});
            if (!result) {
                errors.fetch_add(1);
            } else if (result.value().allowed) {
                allowed.fetch_add(1);
            } else {
                denied.fetch_add(1);
            }
        });
    }
    for (auto& worker : workers) worker.join();

    check(errors.load() == 0, "concurrent sliding window must not return errors");
    check(allowed.load() == 20, "concurrent sliding window must allow exactly the limit");
    check(denied.load() == 20, "concurrent sliding window must deny exactly the excess");
}

void test_token_bucket_concurrency(const std::string& prefix, RedisClient& redis) {
    TokenBucketLimiter limiter(
        redis,
        token_bucket_policy("tb-concurrency", 10, 1),
        prefix,
        60s);

    constexpr int threads = 32;
    std::barrier start(threads);
    std::atomic<int> allowed{0};
    std::atomic<int> denied{0};
    std::atomic<int> errors{0};
    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back([&] {
            start.arrive_and_wait();
            const auto result = limiter.check(CheckRequest{"concurrent-tb-client", 0});
            if (!result) {
                errors.fetch_add(1);
            } else if (result.value().allowed) {
                allowed.fetch_add(1);
            } else {
                denied.fetch_add(1);
            }
        });
    }
    for (auto& worker : workers) worker.join();

    check(errors.load() == 0, "concurrent token bucket must not return errors");
    check(allowed.load() == 10, "concurrent token bucket must allow exactly the capacity");
    check(denied.load() == 22, "concurrent token bucket must deny exactly the excess");
}

void test_validation(const std::string& prefix, RedisClient& redis) {
    TokenBucketLimiter limiter(
        redis,
        token_bucket_policy("tb-validation", 2, 1),
        prefix,
        60s);

    check(
        is_code(limiter.check(CheckRequest{"", 1}), ErrorCode::invalid_client_id),
        "empty client id must be rejected");
    check(
        is_code(limiter.check(CheckRequest{"bad client", 1}), ErrorCode::invalid_client_id),
        "client id with a space must be rejected");
    check(
        is_code(limiter.check(CheckRequest{"bad:client", 1}), ErrorCode::invalid_client_id),
        "client id with a colon must be rejected");
    check(
        is_code(limiter.check(CheckRequest{"valid-client", 3}), ErrorCode::cost_exceeds_limit),
        "cost above max must be rejected");
    check(
        require(limiter.check(CheckRequest{"valid-client", 0}), "default cost").allowed,
        "zero cost must select the policy default cost");
}

void test_registry(const std::string& prefix, RedisClient& redis) {
    RateLimiterRegistry registry;
    registry.add(std::make_unique<TokenBucketLimiter>(
        redis,
        token_bucket_policy("registry-tb", 2, 1),
        prefix,
        60s));

    bool duplicate_threw = false;
    try {
        registry.add(std::make_unique<TokenBucketLimiter>(
            redis,
            token_bucket_policy("registry-tb", 2, 1),
            prefix,
            60s));
    } catch (const std::invalid_argument&) {
        duplicate_threw = true;
    }
    check(duplicate_threw, "registry must reject duplicate policy ids");

    registry.add(std::make_unique<SlidingWindowLimiter>(
        redis,
        sliding_window_policy("registry-sw", 2, 1s),
        prefix));
    registry.seal();

    check(registry.size() == 2, "registry size");
    check(registry.find_policy("registry-tb") != nullptr, "registry must find token bucket policy");
    check(registry.find_policy("registry-sw") != nullptr, "registry must find sliding window policy");
    check(registry.find_policy("missing") == nullptr, "registry must not find missing policy");
    check(
        is_code(registry.check("missing", CheckRequest{"client", 0}), ErrorCode::unknown_policy),
        "registry must return unknown_policy");
    check(
        require(registry.check("registry-tb", CheckRequest{"registry-client", 0}), "registry check").allowed,
        "registry must dispatch to the selected limiter");
}

void test_policy_file() {
    auto policies = load_policies_from_file("config/policies.json");
    const auto& loaded = require(policies, "policy file load");
    check(loaded.size() == 7, "policy file must load all seven configured policies");
    bool found_standard = false;
    for (const auto& policy : loaded) {
        if (policy.id == "api-standard") {
            found_standard = true;
            check(policy.algorithm == Algorithm::token_bucket, "api-standard must be token bucket");
            check(policy_limit(policy) == 100, "api-standard capacity");
        }
    }
    check(found_standard, "policy file must contain api-standard");
}

void test_redis_stats(RedisClient& redis, const std::string& prefix) {
    TokenBucketLimiter limiter(
        redis,
        token_bucket_policy("stats-tb", 1, 1),
        prefix,
        60s);
    require(limiter.check(CheckRequest{"stats-client", 0}), "stats check");

    const auto stats = redis.stats();
    check(stats.pool_size >= 32, "redis pool size");
    check(stats.connections_created > 0, "redis must have created a connection");
    check(stats.commands > 0, "redis must have executed commands");
    check(stats.idle + stats.in_use <= stats.pool_size, "redis pool accounting");
}

} // namespace

int main() {
    try {
        RedisClient redis(redis_config_from_environment(32));
        const auto ping = redis.ping();
        if (!ping) {
            std::cerr << "Redis is required for integration tests: "
                      << ping.error().message << '\n';
            return 2;
        }

        const std::string prefix = unique_prefix();
        test_token_bucket_burst(prefix, redis);
        test_token_bucket_cost(prefix, redis);
        test_token_bucket_refill(prefix, redis);
        test_token_bucket_ttl(prefix, redis);
        test_sliding_window_burst(prefix, redis);
        test_sliding_window_cost(prefix, redis);
        test_sliding_window_concurrency(prefix, redis);
        test_token_bucket_concurrency(prefix, redis);
        test_validation(prefix, redis);
        test_registry(prefix, redis);
        test_policy_file();
        test_redis_stats(redis, prefix);

        if (failures != 0) {
            std::cerr << failures << " test assertion(s) failed\n";
            return 1;
        }
        std::cout << "all core integration tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "test failure: " << error.what() << '\n';
        return 1;
    }
}
