#include "include/db.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <stdexcept>

enum Cleanup {
    Manual = 0,
    Auto = 1
};

template<Cleanup mode>
class TestDir {
    public:
        TestDir(std::filesystem::path directory): directory_(directory) {
            if (std::filesystem::exists(directory)) {
                throw std::runtime_error("Directory already exists");
            }
            std::filesystem::create_directories(directory_);
        }
        TestDir(const TestDir& other) = delete;
        TestDir& operator=(const TestDir& other) = delete;
        std::filesystem::path directory() const { return directory_; }
        ~TestDir() {
            if constexpr (mode == Cleanup::Auto) {
                std::filesystem::remove_all(directory_);
            }
        }
    private:
        const std::filesystem::path directory_;
};

TEST(FileTest, Placeholder) {
    TestDir<Auto> dir("./test_db");
    {
        KVStoreConfig config(100, dir.directory());
        LSMKVStore db(config);
    }
}
