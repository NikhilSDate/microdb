#include "include/logging.hpp"

namespace logging {
    void log(const std::string& message) {
        if constexpr (enable) {
            std::println("\033[1;32m{0}\033[0m", message);
        }
    }
}
