#include <httplib.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <iterator>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

struct Options {
    std::string host{"127.0.0.1"};
    int port{8080};
    std::string path{"/api/v1/check"};
    std::string policy{"api-standard"};
    std::string client_prefix{"bench"};
    std::uint32_t cost{1};
    int threads{8};
    int duration_seconds{10};
};

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

Options parse_options(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string_view argument(argv[i]);
        auto next = [&](std::string_view name) -> std::string {
            if (i + 1 >= argc) {
                throw std::invalid_argument("missing value for " + std::string(name));
            }
            return std::string(argv[++i]);
        };
        if (argument == "--host") options.host = next(argument);
        else if (argument == "--port") options.port = parse_int(next(argument), argument);
        else if (argument == "--path") options.path = next(argument);
        else if (argument == "--policy") options.policy = next(argument);
        else if (argument == "--client-prefix") options.client_prefix = next(argument);
        else if (argument == "--cost") {
            options.cost = static_cast<std::uint32_t>(parse_int(next(argument), argument));
        } else if (argument == "--threads") {
            options.threads = parse_int(next(argument), argument);
        } else if (argument == "--duration") {
            options.duration_seconds = parse_int(next(argument), argument);
        } else {
            throw std::invalid_argument("unknown argument: " + std::string(argument));
        }
    }
    if (options.threads <= 0 || options.threads > 1024) {
        throw std::invalid_argument("threads must be between 1 and 1024");
    }
    if (options.duration_seconds <= 0) {
        throw std::invalid_argument("duration must be positive");
    }
    if (options.cost == 0) {
        throw std::invalid_argument("cost must be positive");
    }
    return options;
}

double percentile(const std::vector<double>& sorted, double quantile) {
    if (sorted.empty()) return 0.0;
    const double position = quantile * static_cast<double>(sorted.size() - 1);
    const auto index = static_cast<std::size_t>(std::ceil(position));
    return sorted[std::min(index, sorted.size() - 1)];
}

} // namespace

int main(int argc, char** argv) {
    try {
        const Options options = parse_options(argc, argv);
        std::atomic<bool> start{false};
        std::atomic<std::uint64_t> responses{0};
        std::atomic<std::uint64_t> errors{0};
        std::atomic<std::uint64_t> status_200{0};
        std::atomic<std::uint64_t> status_429{0};
        std::atomic<std::uint64_t> status_other{0};

        std::vector<std::vector<double>> latencies(static_cast<std::size_t>(options.threads));
        std::vector<std::thread> workers;
        workers.reserve(static_cast<std::size_t>(options.threads));

        for (int thread_index = 0; thread_index < options.threads; ++thread_index) {
            workers.emplace_back([&, thread_index] {
                httplib::Client client(options.host, options.port);
                client.set_keep_alive(true);
                client.set_connection_timeout(2s);
                client.set_read_timeout(10s);
                client.set_write_timeout(10s);
                client.set_follow_location(false);

                const std::string body =
                    "{\"client_id\":\"" + options.client_prefix + "-" +
                    std::to_string(thread_index) + "\",\"policy_id\":\"" +
                    options.policy + "\",\"cost\":" + std::to_string(options.cost) + "}";

                auto& samples = latencies[static_cast<std::size_t>(thread_index)];
                samples.reserve(100'000);

                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }

                const auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(options.duration_seconds);
                while (std::chrono::steady_clock::now() < deadline) {
                    const auto begin = std::chrono::steady_clock::now();
                    const auto response = client.Post(options.path, body, "application/json");
                    const auto end = std::chrono::steady_clock::now();
                    if (!response) {
                        errors.fetch_add(1, std::memory_order_relaxed);
                        std::this_thread::sleep_for(1ms);
                        continue;
                    }
                    samples.push_back(
                        std::chrono::duration<double, std::milli>(end - begin).count());
                    responses.fetch_add(1, std::memory_order_relaxed);
                    if (response->status == 200) {
                        status_200.fetch_add(1, std::memory_order_relaxed);
                    } else if (response->status == 429) {
                        status_429.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        status_other.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }

        const auto test_start = std::chrono::steady_clock::now();
        start.store(true, std::memory_order_release);
        for (auto& worker : workers) worker.join();
        const auto test_end = std::chrono::steady_clock::now();
        const double elapsed = std::chrono::duration<double>(test_end - test_start).count();

        std::vector<double> all_latencies;
        for (auto& samples : latencies) {
            all_latencies.insert(
                all_latencies.end(),
                std::make_move_iterator(samples.begin()),
                std::make_move_iterator(samples.end()));
        }
        std::sort(all_latencies.begin(), all_latencies.end());

        const auto total_responses = responses.load();
        const auto total_errors = errors.load();
        const double rps = elapsed > 0.0 ? static_cast<double>(total_responses) / elapsed : 0.0;

        std::cout << "target=http://" << options.host << ':' << options.port << options.path << '\n';
        std::cout << "policy=" << options.policy << " cost=" << options.cost
                  << " threads=" << options.threads << " duration_s=" << elapsed << '\n';
        std::cout << "responses=" << total_responses << " transport_errors=" << total_errors
                  << " response_rps=" << rps << '\n';
        std::cout << "status_200=" << status_200.load()
                  << " status_429=" << status_429.load()
                  << " status_other=" << status_other.load() << '\n';
        if (!all_latencies.empty()) {
            const double sum = std::accumulate(
                all_latencies.begin(),
                all_latencies.end(),
                0.0);
            std::cout << "latency_ms avg=" << (sum / static_cast<double>(all_latencies.size()))
                      << " p50=" << percentile(all_latencies, 0.50)
                      << " p95=" << percentile(all_latencies, 0.95)
                      << " p99=" << percentile(all_latencies, 0.99)
                      << " max=" << all_latencies.back() << '\n';
        }
        return total_errors == 0 ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << "benchmark failure: " << error.what() << '\n';
        return 1;
    }
}
