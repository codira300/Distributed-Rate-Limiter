#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <mutex>
#include <set>
#include <string>
#include <thread>
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
    const std::string& instance,
    int port,
    const std::string& policy_file,
    const std::string& prefix,
    const std::string& redis_host,
    int redis_port) {
    return {
        "--instance-id", instance,
        "--bind", "127.0.0.1",
        "--port", std::to_string(port),
        "--policies", policy_file,
        "--key-prefix", prefix,
        "--http-threads", "16",
        "--redis-host", redis_host,
        "--redis-port", std::to_string(redis_port),
        "--redis-pool-size", "8",
        "--log-level", "error",
    };
}

} // namespace

int main(int argc, char** argv) {
    try {
        const std::string server_exe = argc > 1 ? argv[1] : "build/debug/rl-server.exe";
        const std::string lb_exe = argc > 2 ? argv[2] : "build/debug/rl-lb.exe";
        const std::string policy_file = argc > 3 ? argv[3] : "config/policies.json";
        const std::string redis_host = env_or("RL_TEST_REDIS_HOST", "127.0.0.1");
        const int redis_port = env_int("RL_TEST_REDIS_PORT", 6379);
        const std::string prefix = "rl:cluster:" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());

        const int base_port = env_int("RL_CLUSTER_PORT_BASE", 18181);
        const int lb_port = base_port + 10;
        std::vector<ChildProcess> children;
        children.reserve(5);

        for (int i = 0; i < 4; ++i) {
            children.emplace_back(
                server_exe,
                server_arguments(
                    "app-" + std::to_string(i + 1),
                    base_port + i,
                    policy_file,
                    prefix,
                    redis_host,
                    redis_port));
        }

        for (int i = 0; i < 4; ++i) {
            const int port = base_port + i;
            if (!rl::test::wait_for_http(port, "/health/live", 15s)) {
                std::cerr << "server on port " << port << " did not become live\n";
                return 1;
            }
            if (!rl::test::wait_for_http(port, "/health/ready", 15s)) {
                std::cerr << "server on port " << port << " did not become ready\n";
                return 1;
            }
        }

        std::string backends;
        for (int i = 0; i < 4; ++i) {
            if (!backends.empty()) backends += ',';
            backends += "127.0.0.1:" + std::to_string(base_port + i);
        }
        children.emplace_back(
            lb_exe,
            std::vector<std::string>{
                "--bind", "127.0.0.1",
                "--port", std::to_string(lb_port),
                "--backends", backends,
                "--threads", "8",
                "--connect-timeout-ms", "2000",
                "--request-timeout-ms", "10000",
            });

        if (!rl::test::wait_for_http(lb_port, "/lb/health", 10s)) {
            std::cerr << "load balancer did not become healthy\n";
            return 1;
        }

        constexpr int workers = 32;
        constexpr int requests_per_worker = 5;
        constexpr int total_requests = workers * requests_per_worker;
        std::barrier start(workers);
        std::atomic<int> allowed{0};
        std::atomic<int> denied{0};
        std::atomic<int> errors{0};
        std::mutex instances_mutex;
        std::set<std::string> instances;
        std::vector<std::thread> threads;
        threads.reserve(workers);

        for (int i = 0; i < workers; ++i) {
            threads.emplace_back([&] {
                start.arrive_and_wait();
                for (int request = 0; request < requests_per_worker; ++request) {
                    try {
                        const auto response = rl::test::post_check(
                            lb_port,
                            "tb-burst-100",
                            "shared-cluster-client",
                            1);
                        if (response.allowed) {
                            allowed.fetch_add(1);
                        } else {
                            denied.fetch_add(1);
                        }
                        if (!response.instance_id.empty()) {
                            std::lock_guard lock(instances_mutex);
                            instances.insert(response.instance_id);
                        }
                    } catch (const std::exception&) {
                        errors.fetch_add(1);
                    }
                }
            });
        }
        for (auto& thread : threads) thread.join();

        if (errors.load() != 0) {
            std::cerr << "cluster requests errored: " << errors.load() << '\n';
            return 1;
        }
        if (allowed.load() != 100) {
            std::cerr << "expected exactly 100 allowed across cluster, got "
                      << allowed.load() << '\n';
            return 1;
        }
        if (denied.load() != total_requests - 100) {
            std::cerr << "expected " << (total_requests - 100)
                      << " denied across cluster, got " << denied.load() << '\n';
            return 1;
        }
        if (instances.size() != 4) {
            std::cerr << "load balancer used " << instances.size()
                      << " instances; expected 4\n";
            return 1;
        }

        for (int i = 0; i < 10; ++i) {
            const auto response = rl::test::post_check(
                lb_port,
                "tb-burst-100",
                "shared-cluster-client",
                1);
            if (response.allowed || response.status != 429) {
                std::cerr << "post-quota request must be denied with HTTP 429\n";
                return 1;
            }
        }

        std::barrier window_start(workers);
        std::atomic<int> window_allowed{0};
        std::atomic<int> window_denied{0};
        std::atomic<int> window_errors{0};
        std::vector<std::thread> window_threads;
        window_threads.reserve(workers);
        for (int i = 0; i < workers; ++i) {
            window_threads.emplace_back([&] {
                window_start.arrive_and_wait();
                for (int request = 0; request < 4; ++request) {
                    try {
                        const auto response = rl::test::post_check(
                            lb_port,
                            "strict-100-per-10s",
                            "cluster-window-client",
                            1);
                        if (response.allowed) window_allowed.fetch_add(1);
                        else window_denied.fetch_add(1);
                    } catch (const std::exception&) {
                        window_errors.fetch_add(1);
                    }
                }
            });
        }
        for (auto& thread : window_threads) thread.join();

        if (window_errors.load() != 0) {
            std::cerr << "sliding window cluster requests errored\n";
            return 1;
        }
        if (window_allowed.load() != 100) {
            std::cerr << "expected exactly 100 sliding-window allows, got "
                      << window_allowed.load() << '\n';
            return 1;
        }

        httplib::Client app_client("127.0.0.1", base_port);
        app_client.set_connection_timeout(2s);
        const auto metrics = app_client.Get("/metrics");
        if (!metrics || metrics->status != 200 ||
            metrics->body.find("rl_decisions_total") == std::string::npos) {
            std::cerr << "instance metrics endpoint did not return expected content\n";
            return 1;
        }

        httplib::Client lb_client("127.0.0.1", lb_port);
        lb_client.set_connection_timeout(2s);
        const auto lb_metrics = lb_client.Get("/lb/metrics");
        if (!lb_metrics || lb_metrics->status != 200 ||
            lb_metrics->body.find("rl_lb_requests_total") == std::string::npos) {
            std::cerr << "load balancer metrics endpoint did not return expected content\n";
            return 1;
        }

        std::cout << "cluster test passed: 4 instances, shared quota, LB round-robin, metrics\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "cluster test failure: " << error.what() << '\n';
        return 1;
    }
}
