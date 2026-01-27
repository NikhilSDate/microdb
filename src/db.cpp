#include "include/db.hpp"
#include <filesystem>
#include <mutex>
#include <print>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <ranges>

std::optional<std::string> MemTable::get(const std::string& k) {
    std::shared_lock<std::shared_mutex> g{lock_};
    if (memtable_.contains(k)) {
        return memtable_.at(k);
    }
    return std::nullopt;
}

void MemTable::put(const std::string& k, const std::string& v) {
    std::unique_lock<std::shared_mutex> g{lock_};
    if (memtable_.contains(k)) {
        size_ = size_ + (v.size() - memtable_.at(k).size());
    } else {
        size_ = size_ + k.size() + v.size();
    }
    memtable_[k] = v;
}

LSMKVStore::LSMKVStore(const KVStoreConfig& config)
    : config_{config}{
    state_ = std::make_shared<LSMStoreState>();

    if (std::filesystem::exists(config_.directory_)) {
        // read all SSTables
        for (auto const& entry: std::filesystem::directory_iterator(config_.directory_)) {
            auto path = entry.path();
            // assuming the database is being used correctly, path should be an SSTable
            SSTable table = SSTable::from_file(path);
            size_t id = table.id();
            state_->sstables_[id] = std::move(table);
            state_->next_sstable_id = std::max(state_->next_sstable_id, table.id() + 1);
        }
    } else {
        if (!std::filesystem::create_directories(config_.directory_)) {
            throw new std::runtime_error("Failed to create directory");
        }
    }
}

std::optional<std::string> LSMKVStore::get(std::string k) {
    // take read lock and read snapshot
    snapshot_lock_.lock_shared();
    auto snapshot = state_;
    snapshot_lock_.unlock_shared();

    auto result = snapshot->memtable_.get(k);
    if (!result.has_value()) {
        // search SSTables
        // iterate in reverse to get more recent tables first
        for (auto& [_, sstable]: snapshot->sstables_ | std::views::reverse) {
            auto res = sstable.get(k);
            // tombstone is 0-length value
            if (res.has_value() && res.value().length() > 0) {
                return res;
            }
        }
        return std::nullopt;
    }
    if (result.value().length() == 0) {
        return std::nullopt;
    }
    return result.value();
}

void LSMKVStore::put(std::string k, std::string v) {
    // need to take read lock on current snapshot
    bool may_flush = false;

    snapshot_lock_.lock_shared();
    auto snapshot = state_;
    snapshot->memtable_.put(k, v);
    if (snapshot->memtable_.size_bytes() > config_.memtable_threshold_) {
        may_flush = true;
    }
    snapshot_lock_.unlock_shared();

    // fast path, may_flush is false (definitely no need to flush)
    if (!may_flush) {
        return;
    }

    // slow path, have to take global lock and check again whether to flush
    state_lock_.lock();
    snapshot_lock_.lock();
    auto state = *state_;
    if (snapshot->memtable_.size_bytes() > this->config_.memtable_threshold_) {
        // need to flush memtable to SSTable
        // this should ideally happen asynchronously

        

        auto sstable = SSTable::from_memtable(next_sstable_id, this->config_.directory_, this->memtable_);
        next_sstable_id++;
        this->sstables_.push_back(std::move(sstable));
        this->memtable_.clear();
    }

    snapshot_lock_.unlock();
    state_lock_.unlock();
}

void LSMKVStore::remove(std::string k) {
    // set a tombstone value
    this->put(k, "");
}

LSMKVStore::~LSMKVStore() {
    // this will create the file, and then close it automatically when the SSTable goes out of scope
    auto _ = SSTable::from_memtable(next_sstable_id, this->config_.directory_, this->memtable_);
}
