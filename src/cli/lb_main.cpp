#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "transport/load_balancer.hpp"

namespace {

int parse_int(std::string_view value, std::string_view name) {
    try {
        std::size_t consumed = 0;
        const int parsed = std::stoi(std::string(value), &consumed, 10);
        if (consumed != value.size()) throw std::invalid_argument("trailing characters");
        return parsed;
    } catch (const std::exception&) {
        throw std::invalid_argument("invalid integer for " + std::string(name));
    }
}

std::vector<rl::Backend> parse_backends(std::string_view value) {
    std::vector<rl::Backend> backends;
    std::size_t begin = 0;
    while (begin <= value.size()) {
        const std::size_t comma = value.find(',', begin);
        const std::string item(value.substr(
            begin,
            comma == std::string_view::npos ? std::string_view::npos : comma - begin));
        if (!item.empty()) {
            const std::size_t colon = item.rfind(':');
            if (colon == std::string::npos || colon == 0 || colon + 1 >= item.size()) {
                throw std::invalid_argument("backend must be host:port: " + item);
            }
            rl::Backend backend;
            backend.host = item.substr(0, colon);
            backend.port = parse_int(std::string_view(item).substr(colon + 1), "backend port");
            if (backend.port <= 0 || backend.port > 65535) {
                throw std::invalid_argument("backend port out of range");
            }
            backends.push_back(std::move(backend));
        }
        if (comma == std::string_view::npos) break;
        begin = comma + 1;
    }
    if (backends.empty()) {
        throw std::invalid_argument("no backends configured");
    }
    return backends;
}

} // namespace

int main(int argc, char** argv) {
    try {
        rl::LoadBalancerConfig config;
        if (const char* env = std::getenv("RL_LB_BIND"); env != nullptr) config.bind_host = env;
        if (const char* env = std::getenv("RL_LB_BACKENDS"); env != nullptr) {
            config.backends = parse_backends(env);
        }

        for (int i = 1; i < argc; ++i) {
            const std::string_view arg(argv[i]);
            auto next = [&](std::string_view name) -> std::string {
                if (i + 1 >= argc) throw std::invalid_argument("missing value for " + std::string(name));
                return std::string(argv[++i]);
            };
            if (arg == "--bind") config.bind_host = next(arg);
            else if (arg == "--port") config.port = parse_int(next(arg), arg);
            else if (arg == "--backends") config.backends = parse_backends(next(arg));
            else if (arg == "--threads") config.threads = parse_int(next(arg), arg);
            else if (arg == "--connect-timeout-ms") {
                config.connection_timeout_ms = parse_int(next(arg), arg);
            } else if (arg == "--request-timeout-ms") {
                config.request_timeout_ms = parse_int(next(arg), arg);
            } else {
                throw std::invalid_argument("unknown argument: " + std::string(arg));
            }
        }

        const std::string address = config.bind_host + ":" + std::to_string(config.port);
        rl::LoadBalancer balancer(std::move(config));
        std::cout << "{\"event\":\"lb_listening\",\"address\":\"" << address << "\"}\n";
        return balancer.run() ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << "fatal: " << error.what() << '\n';
        return 1;
    }
}
