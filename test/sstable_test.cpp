#include "sstable.hpp"
#include "test_utils.hpp"
#include <gtest/gtest.h>
#include <map>

namespace {
// enough entries, each large enough, to span several blocks
std::shared_ptr<SSTable> build_multi_block_table(std::filesystem::path dir, size_t id, size_t num_entries) {
    std::map<std::string, std::string> data;
    for (size_t i = 0; i < num_entries; i++) {
        data[std::format("key{:05d}", i)] = std::string(100, 'a' + (i % 26));
    }
    MemTable<Immutable> memtable(id, data, 0, dir / "unused.wal");
    return std::make_shared<SSTable>(SSTable::from_memtable(id, dir, memtable));
}
}

TEST(SSTable, GET_ALL_KEYS) {
    TestDir dir("get_all_keys");
    auto table = build_multi_block_table(dir.path(), 1, 300);
    ASSERT_GT(table->num_blocks(), 1u);
    for (size_t i = 0; i < 300; i++) {
        auto key = std::format("key{:05d}", i);
        ASSERT_EQ(table->get(key), std::string(100, 'a' + (i % 26)));
    }
    ASSERT_EQ(table->get("missing"), std::nullopt);
}

TEST(SSTable, ROUND_TRIP_THROUGH_FILE) {
    TestDir dir("round_trip");
    build_multi_block_table(dir.path(), 2, 300);

    auto reopened = SSTable::from_file(dir.path() / "sstable-2.sst");
    ASSERT_EQ(reopened.id(), 2u);
    for (size_t i = 0; i < 300; i += 17) {
        auto key = std::format("key{:05d}", i);
        ASSERT_EQ(reopened.get(key), std::string(100, 'a' + (i % 26)));
    }
}

TEST(SSTableIterator, FULL_TRAVERSAL_IS_SORTED) {
    TestDir dir("full_traversal");
    auto table = build_multi_block_table(dir.path(), 3, 300);

    auto it = SSTableIterator::seek_to_first(table);
    size_t count = 0;
    std::string prev;
    while (it.is_valid()) {
        if (count > 0) ASSERT_LT(prev, it.key());
        prev = it.key();
        auto expected_idx = std::stoul(it.key().substr(3));
        EXPECT_EQ(it.value(), std::string(100, 'a' + (expected_idx % 26)));
        count++;
        it.next();
    }
    ASSERT_EQ(count, 300u);
}

TEST(SSTableIterator, SEEK_TO_KEY_MID_TABLE) {
    TestDir dir("seek_mid");
    auto table = build_multi_block_table(dir.path(), 4, 300);

    auto it = SSTableIterator::seek_to_key(table, "key00150");
    ASSERT_TRUE(it.is_valid());
    EXPECT_EQ(it.key(), "key00150");
}

TEST(SSTableIterator, SEEK_TO_KEY_BETWEEN_ENTRIES) {
    TestDir dir("seek_between");
    auto table = build_multi_block_table(dir.path(), 5, 300);

    // "key00150a" doesn't exist; should land on the next real key
    auto it = SSTableIterator::seek_to_key(table, "key00150a");
    ASSERT_TRUE(it.is_valid());
    EXPECT_EQ(it.key(), "key00151");
}

TEST(SSTableIterator, SEEK_PAST_END) {
    TestDir dir("seek_past_end");
    auto table = build_multi_block_table(dir.path(), 6, 300);

    auto it = SSTableIterator::seek_to_key(table, "zzz");
    ASSERT_FALSE(it.is_valid());
}

TEST(SSTableIterator, SEEK_BEFORE_START) {
    TestDir dir("seek_before_start");
    auto table = build_multi_block_table(dir.path(), 7, 300);

    auto it = SSTableIterator::seek_to_key(table, "aaa");
    ASSERT_TRUE(it.is_valid());
    EXPECT_EQ(it.key(), "key00000");
}
