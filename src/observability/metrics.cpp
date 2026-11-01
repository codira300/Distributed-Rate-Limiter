#include "ratelimiter/metrics.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <sstream>
#include <string>
#include <string_view>

namespace rl {

namespace {

constexpr std::size_t kAlgorithmCount = 2;
constexpr std::size_t kStatusCount = 3;
constexpr std::size_t kHttpStatusCount = 600;
constexpr std::array<double, 12> kLatencyBuckets{
    0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5};

std::size_t algorithm_index(Algorithm algorithm) noexcept {
    return algorithm == Algorithm::token_bucket ? 0 : 1;
}

std::size_t status_index(DecisionStatus status) noexcept {
    switch (status) {
    case DecisionStatus::allowed: return 0;
    case DecisionStatus::rate_limited: return 1;
    case DecisionStatus::fail_open: return 2;
    }
    return 0;
}

std::string escape_label(std::string_view input) {
    std::string output;
    output.reserve(input.size() * 2);
    for (const char ch : input) {
        if (ch == '\\' || ch == '"') {
            output.push_back('\\');
        }
        output.push_back(ch);
    }
    return output;
}

struct Histogram {
    std::array<std::atomic<std::uint64_t>, kLatencyBuckets.size()> buckets{};
    std::atomic<std::uint64_t> count{0};
    std::atomic<std::uint64_t> sum_nanoseconds{0};

    void record(std::chrono::nanoseconds latency) noexcept {
        const double seconds = static_cast<double>(latency.count()) / 1'000'000'000.0;
        for (std::size_t i = 0; i < kLatencyBuckets.size(); ++i) {
            if (seconds <= kLatencyBuckets[i]) {
                buckets[i].fetch_add(1, std::memory_order_relaxed);
                break;
            }
        }
        count.fetch_add(1, std::memory_order_relaxed);
        sum_nanoseconds.fetch_add(
            static_cast<std::uint64_t>(latency.count()),
            std::memory_order_relaxed);
    }

    void render(
        std::ostringstream& out,
        std::string_view name,
        std::string_view label_body) const {
        const std::string open = label_body.empty() ? "" : "{" + std::string(label_body) + "}";
        const std::string bucket_prefix = label_body.empty()
            ? std::string(name) + "_bucket{"
            : std::string(name) + "_bucket{" + std::string(label_body) + ",";
        std::uint64_t cumulative = 0;
        for (std::size_t i = 0; i < kLatencyBuckets.size(); ++i) {
            cumulative += buckets[i].load(std::memory_order_relaxed);
            out << bucket_prefix << "le=\"" << kLatencyBuckets[i] << "\"} "
                << cumulative << '\n';
        }
        const auto total = count.load(std::memory_order_relaxed);
        out << bucket_prefix << "le=\"+Inf\"} " << total << '\n';
        out << name << "_count" << open << ' ' << total << '\n';
        out << name << "_sum" << open << ' '
            << static_cast<double>(sum_nanoseconds.load(std::memory_order_relaxed)) / 1'000'000'000.0
            << '\n';
    }
};

} // namespace

struct Metrics::Impl {
    std::array<std::array<std::atomic<std::uint64_t>, kStatusCount>, kAlgorithmCount> decisions{};
    std::array<Histogram, kAlgorithmCount> decision_latency{};
    Histogram redis_latency{};
    std::array<std::atomic<std::uint64_t>, kErrorCodeCount> errors{};
    std::array<std::atomic<std::uint64_t>, kHttpStatusCount> http_status{};
    std::atomic<std::uint64_t> redis_commands{0};
    std::atomic<std::uint64_t> redis_command_errors{0};
    std::atomic<std::int64_t> active_requests{0};
    const std::chrono::steady_clock::time_point started{std::chrono::steady_clock::now()};
};

Metrics::Metrics() : impl_(std::make_unique<Impl>()) {}
Metrics::~Metrics() = default;

void Metrics::record_decision(
    Algorithm algorithm,
    DecisionStatus status,
    std::chrono::nanoseconds latency) {
    const auto ai = algorithm_index(algorithm);
    const auto si = status_index(status);
    impl_->decisions[ai][si].fetch_add(1, std::memory_order_relaxed);
    impl_->decision_latency[ai].record(latency);
}

void Metrics::record_redis_command(std::chrono::nanoseconds latency, bool ok) {
    impl_->redis_commands.fetch_add(1, std::memory_order_relaxed);
    if (!ok) {
        impl_->redis_command_errors.fetch_add(1, std::memory_order_relaxed);
    }
    impl_->redis_latency.record(latency);
}

void Metrics::record_error(ErrorCode code) {
    const auto index = static_cast<std::size_t>(code);
    if (index < kErrorCodeCount) {
        impl_->errors[index].fetch_add(1, std::memory_order_relaxed);
    }
}

void Metrics::record_http_status(int status) {
    if (status >= 0 && status < static_cast<int>(kHttpStatusCount)) {
        impl_->http_status[static_cast<std::size_t>(status)].fetch_add(
            1, std::memory_order_relaxed);
    }
}

void Metrics::increment_active_requests() noexcept {
    impl_->active_requests.fetch_add(1, std::memory_order_relaxed);
}

void Metrics::decrement_active_requests() noexcept {
    impl_->active_requests.fetch_sub(1, std::memory_order_relaxed);
}

std::string Metrics::render_prometheus(
    std::string_view instance_id,
    const RedisStats& redis_stats) const {
    std::ostringstream out;
    const std::string instance = escape_label(instance_id);
    const std::string instance_label = "{instance=\"" + instance + "\"}";

    out << "# HELP rl_instance_info Application instance identity.\n";
    out << "# TYPE rl_instance_info gauge\n";
    out << "rl_instance_info" << instance_label << " 1\n";

    out << "# HELP rl_uptime_seconds Process uptime in seconds.\n";
    out << "# TYPE rl_uptime_seconds gauge\n";
    out << "rl_uptime_seconds" << instance_label << " "
        << std::chrono::duration<double>(std::chrono::steady_clock::now() - impl_->started).count()
        << '\n';

    out << "# HELP rl_decisions_total Rate limit decisions by algorithm and outcome.\n";
    out << "# TYPE rl_decisions_total counter\n";
    static constexpr std::array<std::string_view, kAlgorithmCount> algorithms{
        "token_bucket", "sliding_window"};
    static constexpr std::array<std::string_view, kStatusCount> statuses{
        "allowed", "rate_limited", "fail_open"};
    for (std::size_t ai = 0; ai < kAlgorithmCount; ++ai) {
        for (std::size_t si = 0; si < kStatusCount; ++si) {
            out << "rl_decisions_total{algorithm=\"" << algorithms[ai]
                << "\",outcome=\"" << statuses[si] << "\"} "
                << impl_->decisions[ai][si].load(std::memory_order_relaxed) << '\n';
        }
    }

    out << "# HELP rl_decision_latency_seconds End-to-end decision latency.\n";
    out << "# TYPE rl_decision_latency_seconds histogram\n";
    for (std::size_t ai = 0; ai < kAlgorithmCount; ++ai) {
        const std::string labels = "algorithm=\"" + std::string(algorithms[ai]) + "\"";
        impl_->decision_latency[ai].render(out, "rl_decision_latency_seconds", labels);
    }

    out << "# HELP rl_errors_total Internal and dependency errors by code.\n";
    out << "# TYPE rl_errors_total counter\n";
    for (std::size_t i = 0; i < kErrorCodeCount; ++i) {
        out << "rl_errors_total{code=\"" << to_string(static_cast<ErrorCode>(i))
            << "\"} " << impl_->errors[i].load(std::memory_order_relaxed) << '\n';
    }

    out << "# HELP rl_http_requests_total HTTP responses by status code.\n";
    out << "# TYPE rl_http_requests_total counter\n";
    for (std::size_t status = 200; status < kHttpStatusCount; ++status) {
        const auto value = impl_->http_status[status].load(std::memory_order_relaxed);
        if (value != 0) {
            out << "rl_http_requests_total{status=\"" << status << "\"} " << value << '\n';
        }
    }

    out << "# HELP rl_active_requests Requests currently being processed.\n";
    out << "# TYPE rl_active_requests gauge\n";
    out << "rl_active_requests " << impl_->active_requests.load(std::memory_order_relaxed) << '\n';

    out << "# HELP rl_redis_commands_total Redis commands attempted by this process.\n";
    out << "# TYPE rl_redis_commands_total counter\n";
    out << "rl_redis_commands_total " << impl_->redis_commands.load(std::memory_order_relaxed) << '\n';
    out << "# HELP rl_redis_command_errors_total Redis command failures.\n";
    out << "# TYPE rl_redis_command_errors_total counter\n";
    out << "rl_redis_command_errors_total "
        << impl_->redis_command_errors.load(std::memory_order_relaxed) << '\n';
    out << "# HELP rl_redis_command_latency_seconds Redis command round-trip latency.\n";
    out << "# TYPE rl_redis_command_latency_seconds histogram\n";
    impl_->redis_latency.render(out, "rl_redis_command_latency_seconds", "");

    out << "# HELP rl_redis_pool_size Configured Redis connection pool size.\n";
    out << "# TYPE rl_redis_pool_size gauge\n";
    out << "rl_redis_pool_size " << redis_stats.pool_size << '\n';
    out << "# HELP rl_redis_pool_idle Idle Redis connections.\n";
    out << "# TYPE rl_redis_pool_idle gauge\n";
    out << "rl_redis_pool_idle " << redis_stats.idle << '\n';
    out << "# HELP rl_redis_pool_in_use Redis connections currently leased.\n";
    out << "# TYPE rl_redis_pool_in_use gauge\n";
    out << "rl_redis_pool_in_use " << redis_stats.in_use << '\n';
    out << "# HELP rl_redis_connections_created_total Redis connections opened.\n";
    out << "# TYPE rl_redis_connections_created_total counter\n";
    out << "rl_redis_connections_created_total " << redis_stats.connections_created << '\n';
    out << "# HELP rl_redis_connection_failures_total Redis connection attempts that failed.\n";
    out << "# TYPE rl_redis_connection_failures_total counter\n";
    out << "rl_redis_connection_failures_total " << redis_stats.connection_failures << '\n';
    out << "# HELP rl_redis_pool_acquire_timeouts_total Pool acquisition timeouts.\n";
    out << "# TYPE rl_redis_pool_acquire_timeouts_total counter\n";
    out << "rl_redis_pool_acquire_timeouts_total " << redis_stats.pool_acquire_timeouts << '\n';
    out << "# HELP rl_redis_noscript_recoveries_total EVALSHA NOSCRIPT fallbacks.\n";
    out << "# TYPE rl_redis_noscript_recoveries_total counter\n";
    out << "rl_redis_noscript_recoveries_total " << redis_stats.noscript_recoveries << '\n';

    return out.str();
}

} // namespace rl
