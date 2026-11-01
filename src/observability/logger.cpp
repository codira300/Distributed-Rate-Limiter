#include "ratelimiter/logger.hpp"

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>

namespace rl {

namespace {

std::string escape_json(std::string_view input) {
    std::string output;
    output.reserve(input.size() + 8);
    for (const char ch : input) {
        switch (ch) {
        case '"': output += "\\\""; break;
        case '\\': output += "\\\\"; break;
        case '\n': output += "\\n"; break;
        case '\r': output += "\\r"; break;
        case '\t': output += "\\t"; break;
        default:
            if (static_cast<unsigned char>(ch) < 0x20) {
                output += "?";
            } else {
                output.push_back(ch);
            }
        }
    }
    return output;
}

} // namespace

std::string_view to_string(LogLevel level) noexcept {
    switch (level) {
    case LogLevel::debug: return "debug";
    case LogLevel::info: return "info";
    case LogLevel::warn: return "warn";
    case LogLevel::error: return "error";
    }
    return "info";
}

LogLevel parse_log_level(std::string_view value) {
    if (value == "debug") return LogLevel::debug;
    if (value == "info") return LogLevel::info;
    if (value == "warn" || value == "warning") return LogLevel::warn;
    if (value == "error") return LogLevel::error;
    throw std::invalid_argument("invalid log level: " + std::string(value));
}

Logger::Logger(LogLevel level, std::string instance_id)
    : level_(level), instance_id_(std::move(instance_id)) {}

void Logger::log(LogLevel level, std::string_view event, std::string_view detail) {
    if (static_cast<int>(level) < static_cast<int>(level_)) {
        return;
    }

    const auto now = std::chrono::system_clock::now();
    const auto epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    std::string line;
    line.reserve(128 + detail.size());
    line += "{\"ts_ms\":";
    line += std::to_string(epoch_ms);
    line += ",\"level\":\"";
    line += to_string(level);
    line += "\",\"instance\":\"";
    line += escape_json(instance_id_);
    line += "\",\"event\":\"";
    line += escape_json(event);
    if (!detail.empty()) {
        line += "\",\"detail\":\"";
        line += escape_json(detail);
    }
    line += "\"}\n";

    std::lock_guard lock(mutex_);
    std::cout << line;
    std::cout.flush();
}

} // namespace rl
