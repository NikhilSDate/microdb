#include "sstable.hpp"
#include <gtest/gtest.h>
#include <memory>
#include <vector>

namespace {
std::shared_ptr<Block> build_block(const std::vector<std::pair<std::string, std::string>>& entries) {
    BlockBuilder builder;
    for (auto const& [k, v]: entries) {
        EXPECT_TRUE(builder.push(k, v));
    }
    auto raw = builder.build();
    return std::make_shared<Block>(Block::from_raw(raw));
}
}

TEST(Block, GET_HIT_AND_MISS) {
    auto block = build_block({{"a", "1"}, {"c", "3"}, {"e", "5"}});
    ASSERT_EQ(block->get("a"), "1");
    ASSERT_EQ(block->get("c"), "3");
    ASSERT_EQ(block->get("e"), "5");
    ASSERT_EQ(block->get("b"), std::nullopt);
    ASSERT_EQ(block->get("z"), std::nullopt);
}

TEST(Block, EMPTY_BLOCK) {
    auto block = build_block({});
    ASSERT_EQ(block->num_entries(), 0u);
    ASSERT_EQ(block->get("anything"), std::nullopt);
}

TEST(Block, TOMBSTONE_ENTRY) {
    auto block = build_block({{"a", "1"}, {"b", ""}, {"c", "3"}});
    ASSERT_EQ(block->get("b"), "");
}

TEST(BlockIterator, FORWARD_TRAVERSAL) {
    std::vector<std::pair<std::string, std::string>> entries = {
        {"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}};
    auto block = build_block(entries);

    auto it = BlockIterator::seek_to_first(block);
    for (auto const& [k, v]: entries) {
        ASSERT_TRUE(it.is_valid());
        EXPECT_EQ(it.key(), k);
        EXPECT_EQ(it.value(), v);
        it.next();
    }
    ASSERT_FALSE(it.is_valid());
}

TEST(BlockIterator, SEEK_TO_KEY_EXACT) {
    auto block = build_block({{"a", "1"}, {"c", "3"}, {"e", "5"}});
    auto it = BlockIterator::seek_to_key(block, "c");
    ASSERT_TRUE(it.is_valid());
    EXPECT_EQ(it.key(), "c");
}

TEST(BlockIterator, SEEK_TO_KEY_BETWEEN_ENTRIES) {
    auto block = build_block({{"a", "1"}, {"c", "3"}, {"e", "5"}});
    auto it = BlockIterator::seek_to_key(block, "b");
    ASSERT_TRUE(it.is_valid());
    EXPECT_EQ(it.key(), "c"); // first entry >= "b"
}

TEST(BlockIterator, SEEK_TO_KEY_BEFORE_FIRST) {
    auto block = build_block({{"c", "3"}, {"e", "5"}});
    auto it = BlockIterator::seek_to_key(block, "a");
    ASSERT_TRUE(it.is_valid());
    EXPECT_EQ(it.key(), "c");
}

TEST(BlockIterator, SEEK_TO_KEY_PAST_LAST) {
    auto block = build_block({{"a", "1"}, {"c", "3"}});
    auto it = BlockIterator::seek_to_key(block, "z");
    ASSERT_FALSE(it.is_valid());
}
