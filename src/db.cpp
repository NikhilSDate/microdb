#include "include/db.hpp"
#include <filesystem>
#include <print>
#include <memory>
#include <optional>
#include <stdexcept>

LSMKVStore::LSMKVStore(const KVStoreConfig& config)
    : config_{config}, next_sstable_id{0} {
    if (std::filesystem::exists(config_.directory_)) {
        // read all SSTables
        for (auto const& entry: std::filesystem::directory_iterator(config_.directory_)) {
            auto path = entry.path();
            // assuming the database is being used correctly, path should be an SSTable
            sstables_.push_back(SSTable::from_file(path));
        }
    } else {
        if (!std::filesystem::create_directories(config_.directory_)) {
            throw new std::runtime_error("Failed to create directory");
        }
    }
}

std::optional<std::string> LSMKVStore::get(std::string k) {
    if (!memtable_.contains(k)) {
        // search SSTables
        for (auto& sstable: this->sstables_) {
            auto res = sstable.get(k);
            // tombstone is 0-length value
            if (res.has_value() && res.value().length() > 0) {
                return res;
            }
        }
        return std::nullopt;
    }
    return memtable_.at(k);
}

std::string LSMKVStore::set(std::string k, std::string v) {
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

LSMKVStore::~LSMKVStore() {
    // this will create the file, and then close it automatically when the SSTable goes out of scope
    auto _ = SSTable::from_memtable(next_sstable_id, this->config_.directory_, this->memtable_);
}
