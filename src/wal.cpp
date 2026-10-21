#include "wal.hpp"

#include "posix_io.hpp"
#include <cstdint>
#include <fcntl.h>
#include <fstream>
#include <unistd.h>
#include <utility>

using posix_io::throw_errno;

namespace {
void write_u32(int fd, uint32_t v) { posix_io::write_all(fd, &v, sizeof(v), "WAL write failed"); }
}

WAL WAL::create(std::filesystem::path path) {
    WAL wal;
    wal.fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (wal.fd_ < 0) {
        throw_errno("Failed to create WAL file: " + path.string());
    }
    return wal;
}

WAL::WAL(WAL&& other): fd_(std::exchange(other.fd_, -1)) {}

WAL& WAL::operator=(WAL&& other) {
    std::swap(fd_, other.fd_);
    return *this;
}

WAL::~WAL() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

void WAL::append(const std::string& key, const std::string& value) {
    write_u32(fd_, static_cast<uint32_t>(key.size()));
    posix_io::write_all(fd_, key.data(), key.size(), "WAL write failed");
    write_u32(fd_, static_cast<uint32_t>(value.size()));
    posix_io::write_all(fd_, value.data(), value.size(), "WAL write failed");
    if (::fsync(fd_) != 0) {
        throw_errno("WAL fsync failed");
    }
}

void WAL::replay(const std::filesystem::path& path,
                  const std::function<void(const std::string&, const std::string&)>& on_entry) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return;

    auto read_u32 = [&](uint32_t& v) {
        in.read(reinterpret_cast<char*>(&v), sizeof(v));
        return static_cast<size_t>(in.gcount()) == sizeof(v);
    };

    uint32_t key_len;
    while (read_u32(key_len)) {
        std::string key(key_len, '\0');
        in.read(key.data(), key_len);
        if (static_cast<uint32_t>(in.gcount()) != key_len) break;

        uint32_t val_len;
        if (!read_u32(val_len)) break;
        std::string value(val_len, '\0');
        in.read(value.data(), val_len);
        if (static_cast<uint32_t>(in.gcount()) != val_len) break;

        on_entry(key, value);
    }
}
