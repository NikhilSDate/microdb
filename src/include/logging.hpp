#pragma once

#include <print>
#include <string>

namespace logging {
    void log(const std::string message) {
        std::println("\033[1;32m{0}\033[0m", message);
    }
}