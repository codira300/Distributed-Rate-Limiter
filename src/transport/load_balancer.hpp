#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace httplib {
class Server;
}

namespace rl {

struct Backend {
    std::string host;
    int port{0};
};

struct LoadBalancerConfig {
    std::string bind_host{"0.0.0.0"};
    int port{8080};
    std::vector<Backend> backends;
    int threads{0};
    int connection_timeout_ms{2000};
    int request_timeout_ms{10000};
};

class LoadBalancer {
public:
    explicit LoadBalancer(LoadBalancerConfig config);
    ~LoadBalancer();

    LoadBalancer(const LoadBalancer&) = delete;
    LoadBalancer& operator=(const LoadBalancer&) = delete;

    [[nodiscard]] bool run();

private:
    void configure();

    LoadBalancerConfig config_;
    std::atomic<std::size_t> next_{0};
    std::atomic<std::uint64_t> requests_{0};
    std::atomic<std::uint64_t> failures_{0};
    std::vector<std::atomic<std::uint64_t>> backend_requests_;
    std::unique_ptr<httplib::Server> server_;
};

} // namespace rl
