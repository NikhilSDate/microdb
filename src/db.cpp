#include "include/db.hpp"
#include <optional>

LSMKVStore::LSMKVStore(const KVStoreConfig& config)
    : config_{config} {
}

std::optional<std::string> LSMKVStore::get(std::string k) {
    if (!memtable_->contains(k)) {
        return std::nullopt;
    }
    return memtable_->at(k);
}

std::string LSMKVStore::set(std::string k, std::string v) {
    (*memtable_)[k] = v;
    if (memtable_->size() > this->config_.memtable_threshold_) {
        // need to flush memtable to SSTable
        // this should ideally happen asynchronously
        auto sstable = SSTable::from_memtable(0, this->directory_, *this->memtable_);
        this->sstables_.push_back(std::move(sstable));
        this->memtable_ = std::make_unique<std::map<std::string, std::string>>();
    }
    return v;
}
