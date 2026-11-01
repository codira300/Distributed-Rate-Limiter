#pragma once

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace rl::test {

using namespace std::chrono_literals;

inline std::string quote_argument(const std::string& argument) {
    std::string quoted = "\"";
    for (const char ch : argument) {
        if (ch == '"') quoted += "\\\"";
        else quoted += ch;
    }
    quoted += '"';
    return quoted;
}

class ChildProcess {
public:
    ChildProcess(const std::string& executable, const std::vector<std::string>& arguments) {
        std::string command = quote_argument(executable);
        for (const auto& argument : arguments) {
            command += ' ';
            command += quote_argument(argument);
        }

#ifdef _WIN32
        SECURITY_ATTRIBUTES attributes{};
        attributes.nLength = sizeof(attributes);
        attributes.bInheritHandle = TRUE;
        HANDLE null_handle = CreateFileA(
            "NUL",
            GENERIC_WRITE,
            FILE_SHARE_WRITE,
            &attributes,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            nullptr);
        if (null_handle == INVALID_HANDLE_VALUE) {
            throw std::runtime_error("failed to open NUL device");
        }

        STARTUPINFOA startup{};
        startup.cb = sizeof(startup);
        startup.dwFlags = STARTF_USESTDHANDLES;
        startup.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
        startup.hStdOutput = null_handle;
        startup.hStdError = null_handle;

        PROCESS_INFORMATION info{};
        std::vector<char> mutable_command(command.begin(), command.end());
        mutable_command.push_back('\0');
        const BOOL created = CreateProcessA(
            executable.c_str(),
            mutable_command.data(),
            nullptr,
            nullptr,
            TRUE,
            CREATE_NO_WINDOW,
            nullptr,
            nullptr,
            &startup,
            &info);
        CloseHandle(null_handle);
        if (!created) {
            throw std::runtime_error(
                "failed to start " + executable + " (CreateProcess error " +
                std::to_string(GetLastError()) + ")");
        }
        CloseHandle(info.hThread);
        process_ = info.hProcess;
#else
        const pid_t pid = fork();
        if (pid < 0) {
            throw std::runtime_error("failed to fork for " + executable);
        }
        if (pid == 0) {
            const int null_fd = open("/dev/null", O_WRONLY);
            if (null_fd >= 0) {
                dup2(null_fd, STDOUT_FILENO);
                dup2(null_fd, STDERR_FILENO);
                if (null_fd > STDERR_FILENO) close(null_fd);
            }
            std::vector<char*> argv;
            argv.push_back(const_cast<char*>(executable.c_str()));
            for (const auto& argument : arguments) {
                argv.push_back(const_cast<char*>(argument.c_str()));
            }
            argv.push_back(nullptr);
            execv(executable.c_str(), argv.data());
            _exit(127);
        }
        pid_ = pid;
#endif
    }

    ~ChildProcess() { stop(); }

    ChildProcess(const ChildProcess&) = delete;
    ChildProcess& operator=(const ChildProcess&) = delete;

    ChildProcess(ChildProcess&& other) noexcept { move_from(other); }

    ChildProcess& operator=(ChildProcess&& other) noexcept {
        if (this != &other) {
            stop();
            move_from(other);
        }
        return *this;
    }

    void stop() noexcept {
#ifdef _WIN32
        if (process_ != nullptr) {
            TerminateProcess(static_cast<HANDLE>(process_), 1);
            WaitForSingleObject(static_cast<HANDLE>(process_), 5000);
            CloseHandle(static_cast<HANDLE>(process_));
            process_ = nullptr;
        }
#else
        if (pid_ > 0) {
            kill(pid_, SIGTERM);
            int status = 0;
            waitpid(pid_, &status, 0);
            pid_ = -1;
        }
#endif
    }

private:
    void move_from(ChildProcess& other) noexcept {
#ifdef _WIN32
        process_ = other.process_;
        other.process_ = nullptr;
#else
        pid_ = other.pid_;
        other.pid_ = -1;
#endif
    }

#ifdef _WIN32
    void* process_{nullptr};
#else
    int pid_{-1};
#endif
};

inline bool wait_for_http(int port, const std::string& path, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        httplib::Client client("127.0.0.1", port);
        client.set_connection_timeout(250ms);
        client.set_read_timeout(500ms);
        client.set_write_timeout(500ms);
        if (const auto response = client.Get(path); response && response->status == 200) {
            return true;
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}

struct CheckHttpResponse {
    int status{0};
    bool allowed{false};
    std::uint64_t remaining{0};
    std::string instance_id;
};

inline CheckHttpResponse post_check(
    int port,
    const std::string& policy_id,
    const std::string& client_id,
    std::uint32_t cost = 1) {
    httplib::Client client("127.0.0.1", port);
    client.set_connection_timeout(2s);
    client.set_read_timeout(5s);
    client.set_write_timeout(5s);
    const nlohmann::json body = {
        {"client_id", client_id},
        {"policy_id", policy_id},
        {"cost", cost},
    };
    const auto response = client.Post("/api/v1/check", body.dump(), "application/json");
    if (!response) {
        throw std::runtime_error("rate limit HTTP request failed on port " + std::to_string(port));
    }

    CheckHttpResponse result;
    result.status = response->status;
    result.instance_id = response->get_header_value("X-Instance-Id");
    if (const auto parsed = nlohmann::json::parse(response->body, nullptr, false); parsed.is_object()) {
        result.allowed = parsed.value("allowed", false);
        result.remaining = parsed.value("remaining", 0ULL);
    }
    return result;
}

} // namespace rl::test
