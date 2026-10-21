#pragma once

#include <filesystem>
#include <string>

class TestDir {
public:
    explicit TestDir(const std::string& name)
        : path_(std::filesystem::temp_directory_path() / "microdb-test" / name) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    TestDir(const TestDir&) = delete;
    TestDir& operator=(const TestDir&) = delete;
    ~TestDir() { std::filesystem::remove_all(path_); }

    const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};
