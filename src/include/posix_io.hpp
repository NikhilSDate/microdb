#pragma once

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <string>
#include <unistd.h>

namespace posix_io {

[[noreturn]] inline void throw_errno(const std::string& what) {
    throw std::runtime_error(what + ": " + std::strerror(errno));
}

inline void write_all(int fd, const void* data, size_t len, const std::string& what = "write failed") {
    auto p = static_cast<const char*>(data);
    size_t written = 0;
    while (written < len) {
        ssize_t n = ::write(fd, p + written, len - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw_errno(what);
        }
        written += static_cast<size_t>(n);
    }
}

}
