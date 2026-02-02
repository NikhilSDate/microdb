#pragma once

#include <print>
#include <string>


namespace logging {
    constexpr bool enable = true;

    void log(const std::string& message);
}