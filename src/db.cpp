#include "include/db.hpp"
#include "logging.hpp"
#include "wal.hpp"
#include <cmath>
#include <filesystem>
#include <print>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string_view>
#include <ranges>
#include <thread>
#include <iostream>

namespace {
std::filesystem::path wal_path_for(const std::filesystem::path& directory, size_t id) {
    return directory / std::format("memtable-{0}.wal", id);
}

constexpr std::string_view kWalPrefix = "memtable-";
}

void flush_thread_func(LSMKVStore& store) {
    // block on channel and flush
    while (true) {
        auto item = store.flush_channel_.receive();
        if (item == Stop) {
            break;
        }

        logging::log("flushing");

        store.state_lock_.lock();

        // prepare
        store.snapshot_lock_.lock_shared();
        auto snapshot = store.state_;
        store.snapshot_lock_.unlock_shared();
        auto memtable = snapshot->immutable_memtables_.front();
        auto sstable = std::make_shared<SSTable>(
            SSTable::from_memtable(memtable->id(), store.config_.directory_, *memtable));

        // commit
        store.snapshot_lock_.lock();
        auto state = *store.state_;
        state.immutable_memtables_.pop_front();
        state.sstables_[sstable->id()] = std::move(sstable);
        store.state_ = std::make_shared<LSMStoreState>(std::move(state));
        store.snapshot_lock_.unlock();


        store.state_lock_.unlock();
        
        std::filesystem::remove(memtable->wal_path_);
    }
}

LSMStoreState LSMStoreState::fresh(std::filesystem::path directory) {
    return LSMStoreState(MemTable<Mutable>(0, wal_path_for(directory, 0)), 1);
}

LSMStoreState LSMStoreState::open_dir(std::filesystem::path directory) {
    std::map<size_t, std::shared_ptr<SSTable>> sstables;
    std::map<size_t, std::filesystem::path> wals;
    size_t next_id = 1;

    for (auto const& entry : std::filesystem::directory_iterator(directory)) {
        auto const& path = entry.path();
        if (path.extension() == ".sst") {
            auto table = std::make_shared<SSTable>(SSTable::from_file(path));
            size_t id = table->id();
            next_id = std::max(next_id, id + 1);
            sstables[id] = std::move(table);
        } else if (path.extension() == ".wal") {
            size_t id = std::stoull(path.stem().string().substr(kWalPrefix.size()));
            next_id = std::max(next_id, id + 1);
            wals[id] = path;
        }
    }

    std::map<std::string, std::string> recovered;
    for (auto const& [id, path] : wals) {
        if (sstables.contains(id)) continue;
        WAL::replay(path, [&](const std::string& k, const std::string& v) { recovered[k] = v; });
    }

    size_t new_id = next_id++;
    MemTable<Mutable> memtable(new_id, wal_path_for(directory, new_id));
    for (auto const& [k, v] : recovered) memtable.put(k, v);

    for (auto const& [_, path] : wals) {
        std::filesystem::remove(path);
    }

    LSMStoreState state(std::move(memtable), next_id);
    state.sstables_ = std::move(sstables);
    return state;
}

LSMKVStore::LSMKVStore(const KVStoreConfig& config)
    : config_{config}, flush_thead_{} {
    if (std::filesystem::exists(config_.directory_)) {
        state_ = std::make_shared<LSMStoreState>(LSMStoreState::open_dir(config_.directory_));
    } else {
        if (!std::filesystem::create_directories(config_.directory_)) {
            throw std::runtime_error("Failed to create directory");
        }
        state_ = std::make_shared<LSMStoreState>(LSMStoreState::fresh(config_.directory_));
    }

    // launch flush thread
    flush_thead_ = std::jthread([&]{ flush_thread_func(*this); });
}

std::optional<std::string> LSMKVStore::get(std::string k) {
    // take read lock and read snapshot
    snapshot_lock_.lock_shared();
    auto snapshot = state_;
    snapshot_lock_.unlock_shared();

    auto result = snapshot->memtable_.get(k);
    if (result.has_value()) {
        if (result.value().length() == 0) {
            return std::nullopt;
        }
        return result.value();
    }

    // search immutable memtables
    for (auto& memtable: snapshot->immutable_memtables_ | std::views::reverse) {
        auto result = memtable->get(k);
        if (result.has_value()) {
            if (result.value().length() == 0) {
                return std::nullopt;
            }
            return result.value();
        }
    }

    // search SSTables
    // iterate in reverse to get more recent tables first
    for (auto& [_, sstable]: snapshot->sstables_ | std::views::reverse) {
        auto res = sstable->get(k);
        if (res.has_value()) {
            // tombstone is 0-length value
            if (res.value().length() == 0) {
                return std::nullopt;
            }
            return res;
        }
    }
    return std::nullopt;

}

std::vector<std::pair<std::string, std::string>> LSMKVStore::scan(
    std::optional<std::string> lower, std::optional<std::string> upper) {
    snapshot_lock_.lock_shared();
    auto snapshot = state_;
    snapshot_lock_.unlock_shared();

    std::vector<std::unique_ptr<KVIterator>> sources;

    auto add_memtable_source = [&](std::shared_ptr<MemTable<Immutable>> memtable) {
        sources.push_back(std::make_unique<MapRangeIterator>(
            std::shared_ptr<const std::map<std::string, std::string>>(memtable, &memtable->memtable_), lower));
    };

    add_memtable_source(std::make_shared<MemTable<Immutable>>(snapshot->memtable_.freeze()));
    for (auto& memtable: snapshot->immutable_memtables_ | std::views::reverse) {
        add_memtable_source(memtable);
    }

    for (auto& [_, sstable]: snapshot->sstables_ | std::views::reverse) {
        sources.push_back(std::make_unique<SSTableIterator>(
            lower.has_value() ? SSTableIterator::seek_to_key(sstable, *lower)
                               : SSTableIterator::seek_to_first(sstable)));
    }

    MergeIterator merged(std::move(sources));

    std::vector<std::pair<std::string, std::string>> result;
    while (merged.is_valid()) {
        if (upper.has_value() && merged.key() >= *upper) break;
        if (!merged.value().empty()) {
            result.emplace_back(merged.key(), merged.value());
        }
        merged.next();
    }
    return result;
}

void LSMKVStore::put(std::string k, std::string v) {
    if (v.empty()) {
        throw std::invalid_argument("value must not be empty; use remove() to delete a key");
    }
    put_internal(std::move(k), std::move(v));
}

void LSMKVStore::put_internal(std::string k, std::string v) {
    if (entry_size(k, v) > BLOCK_SIZE) {
        throw std::invalid_argument(std::format(
            "key+value ({0} bytes) does not fit in a {1}-byte block", entry_size(k, v), BLOCK_SIZE));
    }

    // need to take read lock on current snapshot
    bool may_flush = false;

    state_lock_.lock_shared();
    snapshot_lock_.lock_shared();
    auto snapshot = state_;
    size_t new_size = snapshot->memtable_.put(k, v);
    if (new_size > config_.memtable_threshold_) {
        may_flush = true;
    }
    snapshot_lock_.unlock_shared();
    state_lock_.unlock_shared();

    // fast path, may_flush is false (definitely no need to flush)
    if (!may_flush) {
        return;
    }

    // slow path, have to take global lock and check again whether to flush
    // this lock ensures that no two threads will perform the recheck concurrently
    state_lock_.lock();

    // the snapshot's high-level structure
    // cannot change once state_lock_ is taken, as all changes to the snapshot
    // must be performed with the state_lock_ held
    // however, the contents of the memtable within the snapshot could change
    // which is why we take the exclusive lock below when committing the snapshot change

    snapshot_lock_.lock_shared();
    auto slowpath_snapshot = state_;
    snapshot_lock_.unlock_shared();

    if (slowpath_snapshot->memtable_.size_bytes() > this->config_.memtable_threshold_) {

        size_t new_id = slowpath_snapshot->next_table_id();
        auto memtable = MemTable<Mutable>(new_id, wal_path_for(config_.directory_, new_id));
        // the entire read-modify-write on the state is done under the exclusive lock
        // to prevent modifications to the memtable in between the read and write
        snapshot_lock_.lock();
        auto state = *state_; // it should technically be safe to deference slowpath_snapshot here
        state.immutable_memtables_.push_back(
            std::make_shared<MemTable<Immutable>>(state.memtable_.freeze())); // technically this involves a lock but it will be unlocked
        state.memtable_ = std::move(memtable);
        logging::log(std::format("Memtable ID: {0}", memtable.id()));
        state_ = std::make_shared<LSMStoreState>(std::move(state));
        snapshot_lock_.unlock();

        flush_channel_.send(Flush);        
    }

    state_lock_.unlock();
}

void LSMKVStore::remove(std::string k) {
    // set a tombstone value
    put_internal(std::move(k), "");
}

LSMKVStore::~LSMKVStore() {
    flush_channel_.send(Stop);
    if (flush_thead_.joinable()) {
        flush_thead_.join();
    }
    // flush thread has fully exited, so state_ is no longer touched by any other thread
    if (state_->memtable_.size_bytes() > 0) {
        auto wal_path = state_->memtable_.wal_path();
        auto _ = SSTable::from_memtable(state_->next_table_id(), this->config_.directory_, state_->memtable_.freeze());
        std::filesystem::remove(wal_path);
    }
}
