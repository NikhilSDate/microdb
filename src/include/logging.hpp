#pragma once

#include <print>
#include <string>


namespace logging {
    constexpr bool enable = false;

    void log(const std::string& message);
}