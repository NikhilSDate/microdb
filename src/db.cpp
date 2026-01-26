#include "include/db.hpp"
#include <filesystem>
#include <print>
#include <memory>
#include <optional>
#include <stdexcept>
#include <ranges>

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
    state_lock_.lock_shared();
    auto snapshot = state_;
    state_lock_.unlock_shared();

    if (!snapshot->memtable_.contains(k)) {
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
    if (snapshot->memtable_.at(k).length() == 0) {
        return std::nullopt;
    }
    return snapshot->memtable_.at(k);
}

std::string LSMKVStore::put(std::string k, std::string v) {
    memtable_[k] = v;
    if (memtable_.size() > this->config_.memtable_threshold_) {
        // need to flush memtable to SSTable
        // this should ideally happen asynchronously
        auto sstable = SSTable::from_memtable(next_sstable_id, this->config_.directory_, this->memtable_);
        next_sstable_id++;
        this->sstables_.push_back(std::move(sstable));
        this->memtable_.clear();
    }
    return v;
}

void LSMKVStore::remove(std::string k) {
    // set a tombstone value
    set(k, "");
}

LSMKVStore::~LSMKVStore() {
    // this will create the file, and then close it automatically when the SSTable goes out of scope
    auto _ = SSTable::from_memtable(next_sstable_id, this->config_.directory_, this->memtable_);
}
