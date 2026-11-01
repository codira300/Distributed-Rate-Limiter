#pragma once

#include <mutex>
#include <string>
#include <string_view>

namespace rl {

enum class LogLevel { debug, info, warn, error };

[[nodiscard]] std::string_view to_string(LogLevel level) noexcept;
[[nodiscard]] LogLevel parse_log_level(std::string_view value);

class Logger {
public:
    Logger(LogLevel level, std::string instance_id);

    void log(LogLevel level, std::string_view event, std::string_view detail = {});
    void debug(std::string_view event, std::string_view detail = {}) { log(LogLevel::debug, event, detail); }
    void info(std::string_view event, std::string_view detail = {}) { log(LogLevel::info, event, detail); }
    void warn(std::string_view event, std::string_view detail = {}) { log(LogLevel::warn, event, detail); }
    void error(std::string_view event, std::string_view detail = {}) { log(LogLevel::error, event, detail); }

    [[nodiscard]] LogLevel level() const noexcept { return level_; }

private:
    LogLevel level_;
    std::string instance_id_;
    std::mutex mutex_;
};

} // namespace rl
