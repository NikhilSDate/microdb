#pragma once

#include <filesystem>
#include <map>
#include <optional>
#include <shared_mutex>
#include <string>
#include "wal.hpp"

enum MemTableType {
    Mutable,
    Immutable
};

template<MemTableType t>
class MemTable;

template<>
class MemTable<Mutable> {
    // these methods are NOT thread-safe (to allow MemTable to be copied/moved)
public:
    MemTable(const MemTable& other); // this is made explicit because it involves taking the lock

    // Moves are NOT thread safe on the object being moved from
    MemTable(MemTable&& other);

    MemTable& operator=(MemTable other);
    MemTable(size_t id, std::filesystem::path wal_path);

    size_t id() { return id_; };

    std::optional<std::string> get(const std::string& k);
    // returns the memtable's total size after applying this put
    size_t put(const std::string& k, const std::string& v);
    size_t size_bytes() {
        std::shared_lock<std::shared_mutex> g{lock_};
        return size_;
    };
    const std::filesystem::path& wal_path() const { return wal_path_; }

    MemTable<Immutable> freeze();
private:
    // copies/moved-from objects don't own a live WAL handle: only the
    // published, actively-written memtable ever appends to it.
    MemTable(size_t id, std::filesystem::path wal_path, std::optional<WAL> wal)
        : id_{id}, size_{0}, wal_path_{std::move(wal_path)}, wal_{std::move(wal)} {}

    size_t id_;
    mutable std::shared_mutex lock_;
    std::map<std::string, std::string> memtable_;
    size_t size_;
    std::filesystem::path wal_path_;
    std::optional<WAL> wal_;
};

// this is just a record type
template<>
class MemTable<Immutable> {
public:
    MemTable(size_t id, std::map<std::string, std::string> memtable, size_t size, std::filesystem::path wal_path);
    size_t id() { return id_; };
    std::optional<std::string> get(const std::string& k);
    size_t size_bytes() { return size_; };

    size_t id_;
    std::map<std::string, std::string> memtable_;
    size_t size_;
    std::filesystem::path wal_path_;
};
