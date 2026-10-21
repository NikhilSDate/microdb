#pragma once

#include <filesystem>
#include <functional>
#include <string>

class WAL {
public:
    static WAL create(std::filesystem::path path);
    static void replay(const std::filesystem::path& path,
                        const std::function<void(const std::string&, const std::string&)>& on_entry);

    void append(const std::string& key, const std::string& value);

    WAL() = default;
    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;
    WAL(WAL&& other);
    WAL& operator=(WAL&& other);
    ~WAL();

private:
    int fd_ = -1;
};
