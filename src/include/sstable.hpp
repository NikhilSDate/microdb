#pragma once

#include <cstdint>
#include <fstream>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <vector>
#include "memtable.hpp"

const size_t BLOCK_SIZE = 4096; // block size = page size

// new file format
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

    void read(std::span<std::byte> buf, size_t offset, size_t len);
    size_t size() const;

    File() = default;
    File(const File&);
    File& operator=(const File);
    File(File&&);

private:
    std::filesystem::path path_;
    std::ifstream f_;
};

// represents a disk block loaded into memory
class Block {
public:
    static Block from_raw(std::span<std::byte> raw);
    std::optional<std::string> get(const std::string& key) const;
private:
    std::vector<std::byte> data_;
    std::vector<uint32_t> offsets_;
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

    static constexpr size_t SIZE = sizeof(uint16_t) + sizeof(uint32_t) + sizeof(size_t);

    uint16_t block_size;
    uint32_t num_blocks;
    size_t id;
};

class SSTable {
public:
    std::optional<std::string> get(std::string k);
    size_t id() const { return id_; }
    static SSTable from_memtable(size_t id, std::filesystem::path directory, const MemTable<Immutable>& memtable);
    static SSTable from_file(std::filesystem::path filepath);

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
