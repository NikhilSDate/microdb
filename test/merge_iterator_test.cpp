#include "iterator.hpp"
#include <gtest/gtest.h>
#include <vector>

namespace {
class VectorIterator : public KVIterator {
public:
    explicit VectorIterator(std::vector<std::pair<std::string, std::string>> entries)
        : entries_(std::move(entries)) {}

    bool is_valid() const override { return idx_ < entries_.size(); }
    const std::string& key() const override { return entries_[idx_].first; }
    const std::string& value() const override { return entries_[idx_].second; }
    void next() override { idx_++; }

private:
    std::vector<std::pair<std::string, std::string>> entries_;
    size_t idx_ = 0;
};

std::vector<std::pair<std::string, std::string>> drain(MergeIterator& it) {
    std::vector<std::pair<std::string, std::string>> out;
    while (it.is_valid()) {
        out.emplace_back(it.key(), it.value());
        it.next();
    }
    return out;
}
}

TEST(MergeIterator, SINGLE_SOURCE) {
    std::vector<std::unique_ptr<KVIterator>> sources;
    sources.push_back(std::make_unique<VectorIterator>(
        std::vector<std::pair<std::string, std::string>>{{"a", "1"}, {"b", "2"}}));
    MergeIterator merged(std::move(sources));

    auto result = drain(merged);
    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], std::make_pair(std::string("a"), std::string("1")));
    EXPECT_EQ(result[1], std::make_pair(std::string("b"), std::string("2")));
}

TEST(MergeIterator, DISJOINT_SOURCES_INTERLEAVE) {
    std::vector<std::unique_ptr<KVIterator>> sources;
    sources.push_back(std::make_unique<VectorIterator>(
        std::vector<std::pair<std::string, std::string>>{{"a", "1"}, {"c", "3"}}));
    sources.push_back(std::make_unique<VectorIterator>(
        std::vector<std::pair<std::string, std::string>>{{"b", "2"}, {"d", "4"}}));
    MergeIterator merged(std::move(sources));

    auto result = drain(merged);
    ASSERT_EQ(result.size(), 4u);
    EXPECT_EQ(result[0].first, "a");
    EXPECT_EQ(result[1].first, "b");
    EXPECT_EQ(result[2].first, "c");
    EXPECT_EQ(result[3].first, "d");
}

TEST(MergeIterator, DUPLICATE_KEY_NEWEST_SOURCE_WINS) {
    std::vector<std::unique_ptr<KVIterator>> sources;
    // index 0 = most recent source
    sources.push_back(std::make_unique<VectorIterator>(
        std::vector<std::pair<std::string, std::string>>{{"a", "newest"}}));
    sources.push_back(std::make_unique<VectorIterator>(
        std::vector<std::pair<std::string, std::string>>{{"a", "older"}}));
    sources.push_back(std::make_unique<VectorIterator>(
        std::vector<std::pair<std::string, std::string>>{{"a", "oldest"}}));
    MergeIterator merged(std::move(sources));

    auto result = drain(merged);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].second, "newest");
}

TEST(MergeIterator, DUPLICATES_ACROSS_MANY_KEYS) {
    std::vector<std::unique_ptr<KVIterator>> sources;
    sources.push_back(std::make_unique<VectorIterator>(
        std::vector<std::pair<std::string, std::string>>{{"a", "new-a"}, {"c", "new-c"}}));
    sources.push_back(std::make_unique<VectorIterator>(
        std::vector<std::pair<std::string, std::string>>{{"a", "old-a"}, {"b", "old-b"}, {"c", "old-c"}}));
    MergeIterator merged(std::move(sources));

    auto result = drain(merged);
    ASSERT_EQ(result.size(), 3u);
    EXPECT_EQ(result[0], std::make_pair(std::string("a"), std::string("new-a")));
    EXPECT_EQ(result[1], std::make_pair(std::string("b"), std::string("old-b")));
    EXPECT_EQ(result[2], std::make_pair(std::string("c"), std::string("new-c")));
}

TEST(MergeIterator, EMPTY_SOURCES) {
    std::vector<std::unique_ptr<KVIterator>> sources;
    sources.push_back(std::make_unique<VectorIterator>(std::vector<std::pair<std::string, std::string>>{}));
    sources.push_back(std::make_unique<VectorIterator>(
        std::vector<std::pair<std::string, std::string>>{{"a", "1"}}));
    MergeIterator merged(std::move(sources));

    auto result = drain(merged);
    ASSERT_EQ(result.size(), 1u);
}

TEST(MergeIterator, ALL_EMPTY) {
    std::vector<std::unique_ptr<KVIterator>> sources;
    sources.push_back(std::make_unique<VectorIterator>(std::vector<std::pair<std::string, std::string>>{}));
    MergeIterator merged(std::move(sources));
    ASSERT_FALSE(merged.is_valid());
}

TEST(MergeIterator, NO_SOURCES) {
    MergeIterator merged({});
    ASSERT_FALSE(merged.is_valid());
}
