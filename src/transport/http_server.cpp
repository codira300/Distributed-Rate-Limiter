#include "transport/http_server.hpp"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace rl {

namespace {

using json = nlohmann::json;

constexpr std::size_t kMaxBodyBytes = 16 * 1024;

std::string failure_policy(bool fail_open) {
    return fail_open ? "fail-open" : "fail-closed";
}

int status_for(ErrorCode code) {
    switch (code) {
    case ErrorCode::invalid_argument:
    case ErrorCode::invalid_client_id:
    case ErrorCode::invalid_policy:
    case ErrorCode::unknown_policy:
    case ErrorCode::algorithm_mismatch:
    case ErrorCode::cost_exceeds_limit:
        return 400;
    case ErrorCode::redis_unavailable:
    case ErrorCode::redis_timeout:
    case ErrorCode::redis_protocol:
    case ErrorCode::pool_exhausted:
        return 503;
    case ErrorCode::internal:
        return 500;
    }
    return 500;
}

std::string public_error_message(const Error& error) {
    if (is_dependency_error(error.code)) {
        return "rate limiting dependency unavailable";
    }
    if (error.code == ErrorCode::internal) {
        return "internal error";
    }
    return error.message;
}

json error_body(const Error& error, std::string_view instance_id) {
    return json{
        {"error", json{
            {"code", to_string(error.code)},
            {"message", public_error_message(error)},
            {"retryable", error.retryable},
        }},
        {"decision_status", is_dependency_error(error.code)
            ? "dependency_unavailable"
            : "error"},
        {"instance_id", instance_id},
    };
}

json decision_body(const Decision& decision, std::string_view instance_id) {
    return json{
        {"allowed", decision.allowed},
        {"limit", decision.limit},
        {"remaining", decision.remaining},
        {"retry_after_ms", decision.retry_after.count()},
        {"reset_after_ms", decision.reset_after.count()},
        {"algorithm", to_string(decision.algorithm)},
        {"policy_id", decision.policy_id},
        {"decision_status", to_string(decision.status)},
        {"instance_id", instance_id},
    };
}

void set_rate_limit_headers(
    httplib::Response& response,
    const Decision& decision,
    std::string_view instance_id) {
    response.set_header("X-RateLimit-Limit", std::to_string(decision.limit));
    response.set_header("X-RateLimit-Remaining", std::to_string(decision.remaining));
    response.set_header("X-RateLimit-Reset-Ms", std::to_string(decision.reset_after.count()));
    response.set_header("X-RateLimit-Algorithm", std::string(to_string(decision.algorithm)));
    response.set_header("X-RateLimit-Policy", decision.policy_id);
    response.set_header("X-Instance-Id", std::string(instance_id));
    response.set_header("Cache-Control", "no-store");

    if (decision.status == DecisionStatus::rate_limited) {
        const auto seconds = std::max<std::int64_t>(
            1,
            (decision.retry_after.count() + 999) / 1000);
        response.set_header("Retry-After", std::to_string(seconds));
    }
    if (decision.status == DecisionStatus::fail_open) {
        response.set_header("X-RateLimit-Fallback", "fail-open");
    }
}

void record_status(httplib::Response& response, Metrics& metrics, int status) {
    response.status = status;
    metrics.record_http_status(status);
}

void send_error(
    httplib::Response& response,
    Metrics& metrics,
    const Error& error,
    int status,
    std::string_view instance_id) {
    response.set_content(error_body(error, instance_id).dump(), "application/json");
    record_status(response, metrics, status);
}

void send_invalid(
    httplib::Response& response,
    Metrics& metrics,
    std::string message,
    int status,
    std::string_view instance_id) {
    metrics.record_error(ErrorCode::invalid_argument);
    send_error(
        response,
        metrics,
        Error{ErrorCode::invalid_argument, std::move(message), false},
        status,
        instance_id);
}

} // namespace

HttpServer::HttpServer(
    RateLimiterService& service,
    Metrics& metrics,
    RedisClient& redis,
    Logger& logger,
    HttpServerConfig config)
    : service_(service),
      metrics_(metrics),
      redis_(redis),
      logger_(logger),
      config_(std::move(config)),
      server_(std::make_unique<httplib::Server>()) {
    configure();
}

HttpServer::~HttpServer() = default;

void HttpServer::configure() {
    server_->set_payload_max_length(kMaxBodyBytes);
    server_->set_read_timeout(5);
    server_->set_write_timeout(5);
    server_->set_keep_alive_max_count(1000);
    server_->set_keep_alive_timeout(5);
    server_->set_logger([](const httplib::Request&, const httplib::Response&) {});
    if (config_.threads > 0) {
        const int threads = config_.threads;
        server_->new_task_queue = [threads] {
            return new httplib::ThreadPool(static_cast<std::size_t>(threads));
        };
    }

    server_->Post("/api/v1/check", [this](const httplib::Request& request, httplib::Response& response) {
        metrics_.increment_active_requests();
        struct ActiveGuard {
            Metrics& metrics;
            ~ActiveGuard() { metrics.decrement_active_requests(); }
        } guard{metrics_};

        const auto content_type = request.get_header_value("Content-Type");
        if (!content_type.empty() &&
            content_type.find("application/json") == std::string::npos) {
            send_invalid(
                response,
                metrics_,
                "Content-Type must be application/json",
                415,
                service_.instance_id());
            return;
        }

        json body;
        try {
            body = json::parse(request.body);
        } catch (const std::exception&) {
            send_invalid(
                response,
                metrics_,
                "request body is not valid JSON",
                400,
                service_.instance_id());
            return;
        }

        if (!body.is_object() ||
            !body.contains("client_id") || !body.at("client_id").is_string() ||
            !body.contains("policy_id") || !body.at("policy_id").is_string()) {
            send_invalid(
                response,
                metrics_,
                "client_id and policy_id are required strings",
                400,
                service_.instance_id());
            return;
        }

        std::optional<Algorithm> algorithm;
        if (body.contains("algorithm") && !body.at("algorithm").is_null()) {
            if (!body.at("algorithm").is_string()) {
                send_invalid(
                    response,
                    metrics_,
                    "algorithm must be a string",
                    400,
                    service_.instance_id());
                return;
            }
            algorithm = parse_algorithm(body.at("algorithm").get<std::string>());
            if (!algorithm) {
                send_invalid(
                    response,
                    metrics_,
                    "unknown algorithm",
                    400,
                    service_.instance_id());
                return;
            }
        }

        std::optional<std::uint32_t> cost;
        if (body.contains("cost") && !body.at("cost").is_null()) {
            const auto& value = body.at("cost");
            std::uint64_t parsed = 0;
            if (value.is_number_unsigned()) {
                parsed = value.get<std::uint64_t>();
            } else if (value.is_number_integer()) {
                const auto signed_value = value.get<std::int64_t>();
                if (signed_value < 0) {
                    parsed = 0;
                } else {
                    parsed = static_cast<std::uint64_t>(signed_value);
                }
            } else {
                parsed = 0;
            }
            if (parsed == 0 || parsed > 0xFFFF'FFFFULL) {
                send_invalid(
                    response,
                    metrics_,
                    "cost must be a positive integer",
                    400,
                    service_.instance_id());
                return;
            }
            cost = static_cast<std::uint32_t>(parsed);
        }

        const auto result = service_.check(
            body.at("client_id").get<std::string>(),
            body.at("policy_id").get<std::string>(),
            algorithm,
            cost);

        if (!result) {
            const int status = status_for(result.error().code);
            if (status == 503) {
                response.set_header("Retry-After", "1");
                response.set_header("X-RateLimit-Failure-Policy", failure_policy(service_.fail_open_enabled()));
            }
            send_error(response, metrics_, result.error(), status, service_.instance_id());
            return;
        }

        const auto& decision = result.value();
        set_rate_limit_headers(response, decision, service_.instance_id());
        response.set_content(decision_body(decision, service_.instance_id()).dump(), "application/json");
        const int status = decision.status == DecisionStatus::rate_limited ? 429 : 200;
        record_status(response, metrics_, status);
    });

    server_->Get("/health/live", [this](const httplib::Request&, httplib::Response& response) {
        response.set_header("X-Instance-Id", service_.instance_id());
        response.set_header("Cache-Control", "no-store");
        response.set_content(
            json{{"status", "live"}, {"instance_id", service_.instance_id()}}.dump(),
            "application/json");
        record_status(response, metrics_, 200);
    });

    server_->Get("/health/ready", [this](const httplib::Request&, httplib::Response& response) {
        const auto ready = service_.ready();
        response.set_header("X-Instance-Id", service_.instance_id());
        response.set_header("Cache-Control", "no-store");
        if (ready) {
            response.set_content(
                json{
                    {"status", "ready"},
                    {"instance_id", service_.instance_id()},
                    {"redis", "ok"},
                    {"failure_policy", failure_policy(service_.fail_open_enabled())},
                }
                    .dump(),
                "application/json");
            record_status(response, metrics_, 200);
            return;
        }

        metrics_.record_error(ready.error().code);
        response.set_header("X-RateLimit-Failure-Policy", failure_policy(service_.fail_open_enabled()));
        send_error(response, metrics_, ready.error(), 503, service_.instance_id());
    });

    server_->Get("/metrics", [this](const httplib::Request&, httplib::Response& response) {
        response.set_content(
            metrics_.render_prometheus(service_.instance_id(), redis_.stats()),
            "text/plain; version=0.0.4");
        record_status(response, metrics_, 200);
    });

    server_->set_exception_handler(
        [this](const httplib::Request&, httplib::Response& response, std::exception_ptr exception) {
            try {
                if (exception) {
                    std::rethrow_exception(exception);
                }
            } catch (const std::exception& error) {
                logger_.error("unhandled_exception", error.what());
            } catch (...) {
                logger_.error("unhandled_exception", "unknown exception");
            }
            metrics_.record_error(ErrorCode::internal);
            send_error(
                response,
                metrics_,
                Error{ErrorCode::internal, "internal error", false},
                500,
                service_.instance_id());
        });
}

bool HttpServer::run() {
    if (!server_->bind_to_port(config_.bind_host, config_.port)) {
        logger_.error(
            "http_bind_failed",
            "address=" + config_.bind_host + ":" + std::to_string(config_.port));
        return false;
    }

    logger_.info(
        "http_listening",
        "address=" + config_.bind_host + ":" + std::to_string(config_.port) +
            " threads=" + std::to_string(config_.threads));
    return server_->listen_after_bind();
}

} // namespace rl
