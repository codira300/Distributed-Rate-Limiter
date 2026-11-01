#pragma once

#include <memory>
#include <string>

#include "ratelimiter/logger.hpp"
#include "ratelimiter/metrics.hpp"
#include "ratelimiter/redis_client.hpp"
#include "service/rate_limiter_service.hpp"

namespace httplib {
class Server;
}

namespace rl {

struct HttpServerConfig {
    std::string bind_host{"0.0.0.0"};
    int port{8081};
    int threads{0};
};

class HttpServer {
public:
    HttpServer(
        RateLimiterService& service,
        Metrics& metrics,
        RedisClient& redis,
        Logger& logger,
        HttpServerConfig config);
    ~HttpServer();

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    [[nodiscard]] bool run();

private:
    void configure();

    RateLimiterService& service_;
    Metrics& metrics_;
    RedisClient& redis_;
    Logger& logger_;
    HttpServerConfig config_;
    std::unique_ptr<httplib::Server> server_;
};

} // namespace rl
