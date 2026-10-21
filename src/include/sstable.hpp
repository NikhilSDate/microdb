#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include "iterator.hpp"
#include "memtable.hpp"

const size_t BLOCK_SIZE = 4096; // block size = page size

// on-disk size of a key/value entry: keylen(4) key vallen(4) value
inline size_t entry_size(const std::string& key, const std::string& value) {
    return 4 + key.size() + 4 + value.size();
}

// file format
// [B0, B1, B2, B3, ..., B_{N - 1}]
// block size = 1 page (4KB)
// key len, value len are 4B

// data block format
// keylen (4 bytes) key valuelen (4 bytes) value

// metadata format (flat, after data blocks)
// keylen (4 bytes) key keylen (4 bytes) key
// one entry per data block: the first key stored in that block

// file index format (at end of file)
// block size (2 bytes), num blocks (4 bytes), id (8 bytes)

class File {
public:
    static File create(std::filesystem::path, const std::span<std::byte> data);
    static File open(std::filesystem::path path);

    void read(std::span<std::byte> buf, size_t offset, size_t len) const;
    size_t size() const { return size_; }

    File() = default;
    File(const File&);
    File& operator=(File);
    File(File&&);
    ~File();

private:
    std::filesystem::path path_;
    int fd_ = -1;
    size_t size_ = 0;
};

// represents a disk block loaded into memory
class Block {
public:
    static Block from_raw(std::span<std::byte> raw);
    std::optional<std::string> get(const std::string& key) const;

    size_t num_entries() const { return offsets_.size(); }
    std::pair<std::string, std::string> entry_at(size_t idx) const;
    std::string_view key_at(size_t idx) const;
private:
    std::vector<std::byte> data_;
    std::vector<uint32_t> offsets_;
};

class BlockIterator {
public:
    static BlockIterator seek_to_first(std::shared_ptr<const Block> block);
    static BlockIterator seek_to_key(std::shared_ptr<const Block> block, const std::string& key);

    bool is_valid() const { return idx_ < block_->num_entries(); }
    const std::string& key() const { return key_; }
    const std::string& value() const { return value_; }
    void next();

private:
    BlockIterator(std::shared_ptr<const Block> block, size_t idx);
    void load_current();

    std::shared_ptr<const Block> block_;
    size_t idx_;
    std::string key_;
    std::string value_;
};

class BlockBuilder {
public:
    // Returns false if the entry does not fit (block is full).
    bool push(const std::string& key, const std::string& value);
    std::vector<std::byte> build();
    bool empty() const { return data_.empty(); }
private:
    std::vector<std::byte> data_;
};

// sparse index: stores the first key of each data block
class Metadata {
public:
    static Metadata from_raw(std::span<std::byte> raw);
    std::vector<std::byte> to_raw() const;
    size_t lookup_block(const std::string& key) const;
    void add_first_key(const std::string& key) { first_keys_.push_back(key); }
    size_t num_blocks() const { return first_keys_.size(); }
private:
    std::vector<std::string> first_keys_;
};

struct FileIndex {
    static FileIndex from_raw(std::span<std::byte> raw);
    std::vector<std::byte> to_raw() const;

    static constexpr size_t SIZE = sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint64_t);

    uint16_t block_size;
    uint32_t num_blocks;
    uint64_t id;
};

class SSTable {
public:
    std::optional<std::string> get(std::string k);
    size_t id() const { return id_; }
    static SSTable from_memtable(size_t id, std::filesystem::path directory, const MemTable<Immutable>& memtable);
    static SSTable from_file(std::filesystem::path filepath);

    size_t num_blocks() const { return file_index_.num_blocks; }
    size_t find_block(const std::string& key) const { return metadata_.lookup_block(key); }
    std::shared_ptr<Block> load_block(size_t idx) const;

    SSTable() = default;
    SSTable(const SSTable&) = default;
    SSTable& operator=(const SSTable&) = default;
    SSTable(SSTable&&) = default;
    SSTable& operator=(SSTable&&) = default;

private:
    size_t id_ = 0;
    File file_;
    FileIndex file_index_{};
    Metadata metadata_;
};

class SSTableIterator : public KVIterator {
public:
    static SSTableIterator seek_to_first(std::shared_ptr<SSTable> table);
    static SSTableIterator seek_to_key(std::shared_ptr<SSTable> table, const std::string& key);

    bool is_valid() const override { return block_iter_.has_value() && block_iter_->is_valid(); }
    const std::string& key() const override { return block_iter_->key(); }
    const std::string& value() const override { return block_iter_->value(); }
    void next() override;

private:
    SSTableIterator(std::shared_ptr<SSTable> table, size_t block_idx, std::optional<BlockIterator> block_iter);
    void advance_to_next_block();

    std::shared_ptr<SSTable> table_;
    size_t block_idx_;
    std::optional<BlockIterator> block_iter_;
};
