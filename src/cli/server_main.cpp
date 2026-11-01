#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "config/policy_registry.hpp"
#include "config/service_config.hpp"
#include "ratelimiter/logger.hpp"
#include "ratelimiter/metrics.hpp"
#include "ratelimiter/rate_limiter.hpp"
#include "ratelimiter/redis_client.hpp"
#include "ratelimiter/sliding_window.hpp"
#include "ratelimiter/token_bucket.hpp"
#include "service/rate_limiter_service.hpp"
#include "transport/http_server.hpp"

int main(int argc, char** argv) {
    using namespace rl;

    try {
        ServiceConfig config = load_service_config(argc, argv);
        Logger logger(config.log_level, config.instance_id);

        auto policies = load_policies_from_file(config.policy_file);
        if (!policies) {
            logger.error("policy_load_failed", policies.error().message);
            return 1;
        }

        Metrics metrics;
        RedisClient redis(config.redis);

        RateLimiterRegistry registry;
        for (const auto& policy : policies.value()) {
            std::unique_ptr<RateLimiter> limiter;
            if (policy.algorithm == Algorithm::token_bucket) {
                limiter = std::make_unique<TokenBucketLimiter>(
                    redis,
                    policy,
                    config.key_prefix,
                    config.idle_ttl);
            } else {
                limiter = std::make_unique<SlidingWindowLimiter>(
                    redis,
                    policy,
                    config.key_prefix);
            }
            registry.add(std::move(limiter));
        }
        registry.seal();

        RateLimiterService service(
            registry,
            redis,
            metrics,
            logger,
            config.fail_open,
            config.instance_id);

        logger.info(
            "service_configured",
            "policies=" + std::to_string(registry.size()) +
                " redis=" + config.redis.host + ":" + std::to_string(config.redis.port) +
                " pool=" + std::to_string(config.redis.pool_size) +
                " failure_policy=" + (config.fail_open ? std::string("fail-open") : std::string("fail-closed")));

        if (const auto ready = redis.ping(); !ready) {
            logger.warn(
                "redis_not_ready_at_startup",
                "code=" + std::string(to_string(ready.error().code)) +
                    " message=" + ready.error().message);
        } else {
            logger.info("redis_connected");
        }

        HttpServer server(
            service,
            metrics,
            redis,
            logger,
            HttpServerConfig{config.bind_host, config.port, config.http_threads});

        return server.run() ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << "fatal: " << error.what() << '\n';
        return 1;
    }
}
