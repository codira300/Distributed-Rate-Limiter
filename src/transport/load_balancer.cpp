#include "transport/load_balancer.hpp"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace rl {

namespace {

using json = nlohmann::json;

bool equals_ascii_ci(std::string_view lhs, std::string_view rhs) noexcept {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (std::size_t i = 0; i < lhs.size(); ++i) {
        auto left = static_cast<unsigned char>(lhs[i]);
        auto right = static_cast<unsigned char>(rhs[i]);
        if (left >= 'A' && left <= 'Z') {
            left = static_cast<unsigned char>(left + ('a' - 'A'));
        }
        if (right >= 'A' && right <= 'Z') {
            right = static_cast<unsigned char>(right + ('a' - 'A'));
        }
        if (left != right) {
            return false;
        }
    }
    return true;
}

bool is_hop_by_hop(std::string_view name) noexcept {
    return equals_ascii_ci(name, "connection") || equals_ascii_ci(name, "keep-alive") ||
           equals_ascii_ci(name, "proxy-authenticate") ||
           equals_ascii_ci(name, "proxy-authorization") || equals_ascii_ci(name, "te") ||
           equals_ascii_ci(name, "trailer") || equals_ascii_ci(name, "transfer-encoding") ||
           equals_ascii_ci(name, "upgrade") || equals_ascii_ci(name, "content-length") ||
           equals_ascii_ci(name, "host");
}

} // namespace

LoadBalancer::LoadBalancer(LoadBalancerConfig config)
    : config_(std::move(config)),
      backend_requests_(config_.backends.size()),
      server_(std::make_unique<httplib::Server>()) {
    if (config_.backends.empty()) {
        throw std::invalid_argument("at least one backend is required");
    }
    if (config_.port <= 0 || config_.port > 65535) {
        throw std::invalid_argument("load balancer port must be between 1 and 65535");
    }
    if (config_.connection_timeout_ms <= 0 || config_.request_timeout_ms <= 0) {
        throw std::invalid_argument("load balancer timeouts must be positive");
    }
    for (const auto& backend : config_.backends) {
        if (backend.host.empty() || backend.port <= 0 || backend.port > 65535) {
            throw std::invalid_argument("invalid backend address");
        }
    }
    configure();
}

LoadBalancer::~LoadBalancer() = default;

void LoadBalancer::configure() {
    server_->set_payload_max_length(16 * 1024);
    server_->set_read_timeout(10);
    server_->set_write_timeout(10);
    server_->set_keep_alive_max_count(1000);
    server_->set_keep_alive_timeout(10);
    server_->set_logger([](const httplib::Request&, const httplib::Response&) {});
    if (config_.threads > 0) {
        const int threads = config_.threads;
        server_->new_task_queue = [threads] {
            return new httplib::ThreadPool(static_cast<std::size_t>(threads));
        };
    }

    auto proxy = [this](const httplib::Request& request, httplib::Response& response, bool post) {
        requests_.fetch_add(1, std::memory_order_relaxed);
        const std::size_t index = next_.fetch_add(1, std::memory_order_relaxed) % config_.backends.size();
        backend_requests_[index].fetch_add(1, std::memory_order_relaxed);

        static thread_local std::vector<std::unique_ptr<httplib::Client>> clients;
        if (clients.size() != config_.backends.size()) {
            clients.resize(config_.backends.size());
        }

        auto& client = clients[index];
        if (!client) {
            client = std::make_unique<httplib::Client>(
                config_.backends[index].host,
                config_.backends[index].port);
            client->set_keep_alive(true);
            client->set_connection_timeout(
                std::chrono::milliseconds(config_.connection_timeout_ms));
            client->set_read_timeout(
                std::chrono::milliseconds(config_.request_timeout_ms));
            client->set_write_timeout(
                std::chrono::milliseconds(config_.request_timeout_ms));
            client->set_follow_location(false);
            client->set_logger([](const httplib::Request&, const httplib::Response&) {});
        }

        httplib::Headers headers;
        for (const auto& [name, value] : request.headers) {
            if (!is_hop_by_hop(name)) {
                headers.emplace(name, value);
            }
        }
        headers.emplace("X-Forwarded-For", request.remote_addr);

        const std::string content_type = request.get_header_value("Content-Type");
        auto result = post
            ? client->Post(request.target, headers, request.body, content_type)
            : client->Get(request.target, headers);

        if (!result) {
            failures_.fetch_add(1, std::memory_order_relaxed);
            client.reset();
            response.status = 502;
            const json error_body = {
                {"error", {{"code", "bad_gateway"}, {"message", "backend unavailable"}}}};
            response.set_content(error_body.dump(), "application/json");
            return;
        }

        response.status = result->status;
        response.body = result->body;
        for (const auto& [name, value] : result->headers) {
            if (!is_hop_by_hop(name)) {
                response.headers.emplace(name, value);
            }
        }
    };

    server_->Get("/lb/health", [this](const httplib::Request&, httplib::Response& response) {
        response.set_content(
            json{{"status", "ok"}, {"backends", config_.backends.size()}}.dump(),
            "application/json");
    });

    server_->Get("/lb/metrics", [this](const httplib::Request&, httplib::Response& response) {
        std::string output;
        output += "# TYPE rl_lb_requests_total counter\n";
        output += "rl_lb_requests_total " + std::to_string(requests_.load()) + "\n";
        output += "# TYPE rl_lb_backend_failures_total counter\n";
        output += "rl_lb_backend_failures_total " + std::to_string(failures_.load()) + "\n";
        output += "# TYPE rl_lb_backend_requests_total counter\n";
        for (std::size_t i = 0; i < config_.backends.size(); ++i) {
            output += "rl_lb_backend_requests_total{backend=\"" +
                config_.backends[i].host + ":" + std::to_string(config_.backends[i].port) +
                "\"} " + std::to_string(backend_requests_[i].load()) + "\n";
        }
        response.set_content(output, "text/plain; version=0.0.4");
    });

    server_->Post(".*", [proxy](const httplib::Request& request, httplib::Response& response) {
        proxy(request, response, true);
    });
    server_->Get(".*", [proxy](const httplib::Request& request, httplib::Response& response) {
        proxy(request, response, false);
    });
}

bool LoadBalancer::run() {
    if (!server_->bind_to_port(config_.bind_host, config_.port)) {
        return false;
    }
    return server_->listen_after_bind();
}

} // namespace rl
