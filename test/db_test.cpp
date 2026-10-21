#include "db.hpp"
#include "test_utils.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {
std::string key(size_t i) { return std::format("key{:03d}", i); }
std::string val(size_t i) { return std::format("value{:03d}", i); }
}

TEST(DB, TEST_CREATE) {
    TestDir dir(::testing::UnitTest::GetInstance()->current_test_info()->name());
    KVStoreConfig config(100, dir.path() / "db");
    LSMKVStore db(config);
}

TEST(DB, TEST_GET_AND_PUT) {
    TestDir dir(::testing::UnitTest::GetInstance()->current_test_info()->name());
    constexpr int keys = 50;
    KVStoreConfig config(512, dir.path() / "db");
    {
        LSMKVStore db(config);
        for (size_t i = 0; i < keys; i++) {
            db.put(key(i), val(i));
        }
    }
    {
        LSMKVStore db(config);
        for (size_t i = 0; i < keys; i++) {
            ASSERT_EQ(db.get(key(i)), val(i));
        }
    }
}

TEST(DB, TEST_DELETE) {
    TestDir dir(::testing::UnitTest::GetInstance()->current_test_info()->name());
    constexpr int keys = 50;
    std::map<std::string, std::string> ground_truth;
    KVStoreConfig config(512, dir.path());
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
            auto gt = ground_truth.contains(key(i)) ? std::make_optional(ground_truth.at(key(i))) : std::nullopt;
            ASSERT_EQ(db.get(key(i)), gt);
        }
    }
}

TEST(DB, TEST_EMPTY_VALUE_REJECTED) {
    TestDir dir(::testing::UnitTest::GetInstance()->current_test_info()->name());
    KVStoreConfig config(512, dir.path());
    LSMKVStore db(config);
    ASSERT_THROW(db.put("k", ""), std::invalid_argument);
    db.put("k", "v");
    db.remove("k");
    ASSERT_EQ(db.get("k"), std::nullopt);
}

TEST(DB, TEST_OVERSIZED_ENTRY_REJECTED) {
    TestDir dir(::testing::UnitTest::GetInstance()->current_test_info()->name());
    KVStoreConfig config(512, dir.path());
    LSMKVStore db(config);
    ASSERT_THROW(db.put("k", std::string(BLOCK_SIZE, 'x')), std::invalid_argument);
    ASSERT_EQ(db.get("k"), std::nullopt);
}

TEST(DB, TEST_REGISTRY_CLEANUP) {
    TestDir dir(::testing::UnitTest::GetInstance()->current_test_info()->name());
    {
        KVStoreConfig config(200, dir.path());
        LSMKVStore db(config);
        for (size_t i = 0; i < 100; i++) db.put(key(i), val(i));
    }
    size_t wal_count = 0;
    for (auto const& entry: std::filesystem::directory_iterator(dir.path())) {
        if (entry.path().extension() == ".wal") wal_count++;
    }
    ASSERT_EQ(wal_count, 0u);
}

TEST(DB, TEST_WAL_CRASH_RECOVERY) {
    TestDir dir(::testing::UnitTest::GetInstance()->current_test_info()->name());
    constexpr int keys = 50;
    KVStoreConfig config(1 << 20, dir.path()); // large threshold: nothing auto-flushes
    {
        auto* db = new LSMKVStore(config);
        for (size_t i = 0; i < keys; i++) db->put(key(i), val(i));
        // deliberately leaked: simulates a crash before clean shutdown
    }
    LSMKVStore recovered(config);
    for (size_t i = 0; i < keys; i++) {
        ASSERT_EQ(recovered.get(key(i)), val(i));
    }
}

TEST(DB, TEST_CONCURRENT_PUT_STRESS) {
    TestDir dir(::testing::UnitTest::GetInstance()->current_test_info()->name());
    KVStoreConfig config(256, dir.path());
    constexpr int per_thread = 300;
    {
        LSMKVStore db(config);
        std::vector<std::jthread> writers;
        for (int t = 0; t < 4; t++) {
            writers.emplace_back([&db, t] {
                for (int i = 0; i < per_thread; i++) {
                    db.put(std::format("t{}-{}", t, key(i)), val(i));
                }
            });
        }
        writers.clear();
        for (int t = 0; t < 4; t++) {
            for (int i = 0; i < per_thread; i++) {
                ASSERT_EQ(db.get(std::format("t{}-{}", t, key(i))), val(i));
            }
        }
    }
    {
        LSMKVStore db(config);
        for (int t = 0; t < 4; t++) {
            for (int i = 0; i < per_thread; i++) {
                ASSERT_EQ(db.get(std::format("t{}-{}", t, key(i))), val(i));
            }
        }
    }
}

TEST(DB, TEST_SCAN_BASIC) {
    TestDir dir(::testing::UnitTest::GetInstance()->current_test_info()->name());
    KVStoreConfig config(1 << 20, dir.path());
    LSMKVStore db(config);
    for (size_t i = 0; i < 20; i++) db.put(key(i), val(i));

    auto results = db.scan();
    ASSERT_EQ(results.size(), 20u);
    for (size_t i = 0; i < 20; i++) {
        EXPECT_EQ(results[i].first, key(i));
        EXPECT_EQ(results[i].second, val(i));
    }
}

TEST(DB, TEST_SCAN_BOUNDS) {
    TestDir dir(::testing::UnitTest::GetInstance()->current_test_info()->name());
    KVStoreConfig config(1 << 20, dir.path());
    LSMKVStore db(config);
    for (size_t i = 0; i < 20; i++) db.put(key(i), val(i));

    auto results = db.scan(key(5), key(10));
    ASSERT_EQ(results.size(), 5u);
    for (size_t i = 0; i < 5; i++) {
        EXPECT_EQ(results[i].first, key(5 + i));
    }

    auto lower_only = db.scan(key(18));
    ASSERT_EQ(lower_only.size(), 2u);
    EXPECT_EQ(lower_only[0].first, key(18));
    EXPECT_EQ(lower_only[1].first, key(19));

    auto upper_only = db.scan(std::nullopt, key(2));
    ASSERT_EQ(upper_only.size(), 2u);
    EXPECT_EQ(upper_only[0].first, key(0));
    EXPECT_EQ(upper_only[1].first, key(1));
}

TEST(DB, TEST_SCAN_TOMBSTONES) {
    TestDir dir(::testing::UnitTest::GetInstance()->current_test_info()->name());
    KVStoreConfig config(1 << 20, dir.path());
    LSMKVStore db(config);
    for (size_t i = 0; i < 10; i++) db.put(key(i), val(i));
    db.remove(key(3));
    db.remove(key(7));

    auto results = db.scan();
    ASSERT_EQ(results.size(), 8u);
    for (auto const& [k, v]: results) {
        EXPECT_NE(k, key(3));
        EXPECT_NE(k, key(7));
    }
}

TEST(DB, TEST_SCAN_ACROSS_GENERATIONS) {
    TestDir dir(::testing::UnitTest::GetInstance()->current_test_info()->name());
    // small threshold: forces flushes across memtable generations and SSTables
    KVStoreConfig config(256, dir.path());
    LSMKVStore db(config);

    std::map<std::string, std::string> ground_truth;
    for (size_t i = 0; i < 200; i++) {
        db.put(key(i), val(i));
        ground_truth[key(i)] = val(i);
    }
    for (size_t i = 0; i < 200; i += 7) {
        db.remove(key(i));
        ground_truth.erase(key(i));
    }
    for (size_t i = 100; i < 120; i++) {
        auto updated = std::format("updated{:03d}", i);
        db.put(key(i), updated);
        ground_truth[key(i)] = updated;
    }

    auto results = db.scan();
    ASSERT_EQ(results.size(), ground_truth.size());
    size_t idx = 0;
    for (auto const& [k, v]: ground_truth) {
        EXPECT_EQ(results[idx].first, k);
        EXPECT_EQ(results[idx].second, v);
        idx++;
    }
}
