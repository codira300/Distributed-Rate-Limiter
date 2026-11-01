#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

#include "http_test_utils.hpp"

namespace {

using namespace std::chrono_literals;
using rl::test::ChildProcess;

std::string env_or(const char* name, const std::string& fallback) {
    if (const char* value = std::getenv(name); value != nullptr) return value;
    return fallback;
}

int env_int(const char* name, int fallback) {
    if (const char* value = std::getenv(name); value != nullptr) return std::stoi(value);
    return fallback;
}

std::vector<std::string> server_arguments(
    int port,
    const std::string& policy_file,
    const std::string& prefix,
    int redis_port,
    bool fail_open) {
    std::vector<std::string> arguments{
        "--instance-id", fail_open ? "failure-open" : "failure-closed",
        "--bind", "127.0.0.1",
        "--port", std::to_string(port),
        "--policies", policy_file,
        "--key-prefix", prefix,
        "--http-threads", "2",
        "--redis-host", "127.0.0.1",
        "--redis-port", std::to_string(redis_port),
        "--redis-pool-size", "4",
        "--redis-connect-timeout-ms", "100",
        "--redis-command-timeout-ms", "200",
        "--redis-acquire-timeout-ms", "200",
        "--log-level", "error",
    };
    arguments.push_back(fail_open ? "--fail-open" : "--fail-closed");
    return arguments;
}

httplib::Result post_raw(int port, const std::string& body) {
    httplib::Client client("127.0.0.1", port);
    client.set_connection_timeout(2s);
    client.set_read_timeout(5s);
    client.set_write_timeout(5s);
    return client.Post("/api/v1/check", body, "application/json");
}

} // namespace

int main(int argc, char** argv) {
    try {
        const std::string server_exe = argc > 1 ? argv[1] : "build/debug/rl-server.exe";
        const std::string policy_file = argc > 2 ? argv[2] : "config/policies.json";
        const int closed_port = env_int("RL_FAILURE_CLOSED_PORT", 18281);
        const int open_port = env_int("RL_FAILURE_OPEN_PORT", 18282);
        const int dead_redis_port = env_int("RL_DEAD_REDIS_PORT", 16399);
        const std::string prefix = "rl:failure:" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());

        {
            ChildProcess closed(
                server_exe,
                server_arguments(closed_port, policy_file, prefix, dead_redis_port, false));
            if (!rl::test::wait_for_http(closed_port, "/health/live", 10s)) {
                std::cerr << "fail-closed server did not become live\n";
                return 1;
            }

            httplib::Client health("127.0.0.1", closed_port);
            health.set_connection_timeout(2s);
            const auto ready = health.Get("/health/ready");
            if (!ready || ready->status != 503) {
                std::cerr << "fail-closed server must report unready when Redis is unavailable\n";
                return 1;
            }

            const auto response = post_raw(
                closed_port,
                R"({"client_id":"failure-client","policy_id":"api-standard","cost":1})");
            if (!response) {
                std::cerr << "fail-closed request failed at the HTTP layer\n";
                return 1;
            }
            if (response->status != 503) {
                std::cerr << "fail-closed server must return 503, got "
                          << response->status << '\n';
                return 1;
            }
            if (response->get_header_value("X-RateLimit-Failure-Policy") != "fail-closed") {
                std::cerr << "missing fail-closed policy response header\n";
                return 1;
            }
            if (!response->get_header_value("X-RateLimit-Fallback").empty()) {
                std::cerr << "fail-closed response must not advertise a fallback\n";
                return 1;
            }
            if (response->body.find("dependency_unavailable") == std::string::npos) {
                std::cerr << "fail-closed response must identify dependency_unavailable\n";
                return 1;
            }
        }

        {
            ChildProcess open(
                server_exe,
                server_arguments(open_port, policy_file, prefix, dead_redis_port, true));
            if (!rl::test::wait_for_http(open_port, "/health/live", 10s)) {
                std::cerr << "fail-open server did not become live\n";
                return 1;
            }

            httplib::Client health("127.0.0.1", open_port);
            health.set_connection_timeout(2s);
            const auto ready = health.Get("/health/ready");
            if (!ready || ready->status != 503) {
                std::cerr << "fail-open server must still report unready when Redis is unavailable\n";
                return 1;
            }

            const auto response = post_raw(
                open_port,
                R"({"client_id":"failure-client","policy_id":"api-standard","cost":1})");
            if (!response) {
                std::cerr << "fail-open request failed at the HTTP layer\n";
                return 1;
            }
            if (response->status != 200) {
                std::cerr << "fail-open server must serve the request, got "
                          << response->status << '\n';
                return 1;
            }
            if (response->get_header_value("X-RateLimit-Fallback") != "fail-open") {
                std::cerr << "missing fail-open fallback response header\n";
                return 1;
            }
            if (response->body.find("fail_open") == std::string::npos) {
                std::cerr << "fail-open response must report fail_open decision status\n";
                return 1;
            }
        }

        std::cout << "failure test passed: fail-closed 503 and fail-open 200 behavior verified\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "failure test failure: " << error.what() << '\n';
        return 1;
    }
}
