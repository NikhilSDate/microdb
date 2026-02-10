#include "include/db.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
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

TEST(DB, TEST_CREATE) {
    auto name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    auto path = std::filesystem::path("./test").append(name);
    TestDir<Auto> dir(path);
    {
        KVStoreConfig config(100, dir.directory().append("db"));
        LSMKVStore db(config);
    }
}

TEST(DB, TEST_GET_AND_PUT) {
    auto name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    auto path = std::filesystem::path("./test").append(name);
    TestDir<Auto> dir(path);
    {
        constexpr int keys = 50;
        KVStoreConfig config(512, dir.directory().append("db"));
        {
            LSMKVStore db(config);
            for (size_t i = 0; i < keys; i++) {
                auto key = std::format("key{:03d}", i);
                auto value = std::format("value{:03d}", i);
                db.put(key, value);
            }
        }
        {
            LSMKVStore db(config);

            size_t found = 0;
            size_t not_found = 0;
            size_t mismatched = 0;

            for (size_t i = 0; i < keys; i++) {
                auto key = std::format("key{:03d}", i);
                auto expected_value = std::format("value{:03d}", i);
                auto result = db.get(key);

                if (!result.has_value()) {
                    not_found++;
                } else if (result.value() != expected_value) {
                    mismatched++;
                } else {
                    found++;
                }
            }
            ASSERT_EQ(mismatched, 0);
            ASSERT_EQ(not_found, 0);
            ASSERT_EQ(found, keys);
        }
    }
}

TEST(DB, TEST_DELETE) {
    auto name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    auto path = std::filesystem::path("./test").append(name);
    TestDir<Manual> dir(path);
    auto key = [](size_t i) {return std::format("key{:03d}", i); };
    auto val = [](size_t i) {return std::format("value{:03d}", i); };
    constexpr int keys = 50;
    std::map<std::string, std::string> ground_truth;
    {
        KVStoreConfig config(512, dir.directory());
        {
            LSMKVStore db(config);
            for (size_t i = 0; i < keys; i++) {
                db.put(key(i), val(i));
                ground_truth[key(i)] = val(i);
            }
            for (size_t i = 0; i < keys; i += 10) {
                db.remove(key(i));
                ASSERT_EQ(db.get(key(i)), std::nullopt);
                ground_truth.erase(key(i));
            }
        }
        {
            LSMKVStore db(config);
            for (size_t i = 0; i < keys; i++) {
                auto v = db.get(key(i));
                auto gt = ground_truth.contains(key(i)) ? std::make_optional(ground_truth.at(key(i))) : std::nullopt;
                ASSERT_EQ(v, gt);
            }
        }  
    }
}
