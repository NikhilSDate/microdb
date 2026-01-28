#include "include/memtable.hpp"
#include <mutex>
#include <optional>
#include <shared_mutex>

// MemTable<Mutable> implementations
MemTable<Mutable>::MemTable(const MemTable& other): MemTable(other.id_) {
    std::shared_lock<std::shared_mutex> g{other.lock_};
    this->memtable_ = other.memtable_;
    this->size_ = other.size_;
}

MemTable<Mutable>::MemTable(MemTable&& other): MemTable(other.id_) {
    std::swap(this->memtable_, other.memtable_);
    std::swap(this->size_, other.size_);
}

MemTable<Mutable>& MemTable<Mutable>::operator=(MemTable other) {
    std::swap(this->memtable_, other.memtable_);
    std::swap(this->size_, other.size_);
    return *this;
}

std::optional<std::string> MemTable<Mutable>::get(const std::string& k) {
    std::shared_lock<std::shared_mutex> g{lock_};
    if (memtable_.contains(k)) {
        return memtable_.at(k);
    }
    return std::nullopt;
}

void MemTable<Mutable>::put(const std::string& k, const std::string& v) {
    std::unique_lock<std::shared_mutex> g{lock_};
    if (memtable_.contains(k)) {
        size_ = size_ + (v.size() - memtable_.at(k).size());
    } else {
        size_ = size_ + k.size() + v.size();
    }
    memtable_[k] = v;
}

MemTable<Immutable> MemTable<Mutable>::freeze() {
    std::shared_lock<std::shared_mutex> g{lock_};
    return MemTable<Immutable>(id_, memtable_, size_);
}

// MemTable<Immutable> implementations
MemTable<Immutable>::MemTable(size_t id, std::map<std::string, std::string> memtable, size_t size)
    : id_{id}, memtable_{std::move(memtable)}, size_{size} {}

std::optional<std::string> MemTable<Immutable>::get(const std::string& k) {
    if (memtable_.contains(k)) {
        return memtable_.at(k);
    }
    return std::nullopt;
}
