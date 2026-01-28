#pragma once

#include <map>
#include <optional>
#include <shared_mutex>
#include <string>

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
    MemTable(size_t id): id_{id}, size_{0} {};

    size_t id() { return id_; };

    std::optional<std::string> get(const std::string& k);
    void put(const std::string& k, const std::string& v);
    size_t size_bytes() { return size_; };
private:
    size_t id_;
    mutable std::shared_mutex lock_;
    std::map<std::string, std::string> memtable_;
    size_t size_;
};

template<>
class MemTable<Immutable> {
public:
    MemTable() = delete;
    size_t id() { return id_; };
    std::optional<std::string> get(const std::string& k);
    size_t size_bytes() { return size_; };
private:
    size_t id_;
    std::map<std::string, std::string> memtable_;
    size_t size_;
};
