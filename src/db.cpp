#include "include/db.hpp"
#include "logging.hpp"
#include <filesystem>
#include <print>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <ranges>
#include <thread>
#include <iostream>

void flush_thread_func(LSMKVStore& store) {
    // block on channel and flush 
    while (true) {
        auto item = store.flush_channel_.receive();
        if (item == Stop) {
            break;
        }

        std::cout << "flushing" << std::endl;

        store.state_lock_.lock();

        // prepare
        store.snapshot_lock_.lock_shared();
        auto snapshot = store.state_;
        store.snapshot_lock_.unlock_shared();
        auto memtable = snapshot->immutable_memtables_.front();
        auto sstable = SSTable::from_memtable(memtable.id(), store.config_.directory_, memtable);

        // commit
        store.snapshot_lock_.lock();
        auto state = *store.state_;
        state.immutable_memtables_.pop_front();
        state.sstables_[sstable.id()] = std::move(sstable);
        store.state_ = std::make_shared<LSMStoreState>(std::move(state));
        store.snapshot_lock_.unlock();
        
        
        store.state_lock_.unlock();
    }
}

LSMStoreState LSMStoreState::open_dir(std::filesystem::path directory) {
    LSMStoreState state;
    for (auto const& entry: std::filesystem::directory_iterator(directory)) {
        auto path = entry.path();
        // assuming the database is being used correctly, path should be an SSTable
        SSTable table = SSTable::from_file(path);
        size_t id = table.id();
        state.sstables_[id] = std::move(table);
        state.next_table_id_ = std::max(state.next_table_id_, table.id() + 1);
    }
    return state;
}

LSMKVStore::LSMKVStore(const KVStoreConfig& config)
    : config_{config}, flush_thead_{} {
    if (std::filesystem::exists(config_.directory_)) {
        // read all SSTables
        state_ = std::make_shared<LSMStoreState>(LSMStoreState::open_dir(config_.directory_));
    } else {
        if (!std::filesystem::create_directories(config_.directory_)) {
            throw new std::runtime_error("Failed to create directory");
        }
        state_ = std::make_shared<LSMStoreState>();
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
        auto result = memtable.get(k);
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
        auto res = sstable.get(k);
        // tombstone is 0-length value
        if (res.has_value() && res.value().length() > 0) {
            return res;
        }
    }
    return std::nullopt;

}

void LSMKVStore::put(std::string k, std::string v) {
    // need to take read lock on current snapshot
    bool may_flush = false;

    state_lock_.lock_shared();
    snapshot_lock_.lock_shared();
    auto snapshot = state_;
    snapshot->memtable_.put(k, v);
    if (snapshot->memtable_.size_bytes() > config_.memtable_threshold_) {
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

    // currently this two-part locking doesnt serve much of a purpose, but would be needed if we wanted to 
    // create a WAL for the memtable, which might be slow
    // this locking approach allows us to create the MemTable (and WAL) outside the lock
    // though perhaps it's better to create a placeholder memtable and put in the ID laterx`

    if (slowpath_snapshot->memtable_.size_bytes() > this->config_.memtable_threshold_) {

        auto memtable = MemTable<Mutable>(slowpath_snapshot->next_table_id());

        // the entire read-modify-write on the state is done under the exclusive lock
        // to prevent modifications to the memtable in between the read and write
        snapshot_lock_.lock();
        auto state = *state_; // it should technically be safe to deference slowpath_snapshot here
        state.immutable_memtables_.push_back(state.memtable_.freeze()); // technically this involves a lock but it will be unlocked
        state.memtable_ = std::move(memtable);
        state_ = std::make_shared<LSMStoreState>(std::move(state));
        snapshot_lock_.unlock();

        flush_channel_.send(Flush);        
    }

    state_lock_.unlock();
}

void LSMKVStore::remove(std::string k) {
    // set a tombstone value
    this->put(k, "");
}

LSMKVStore::~LSMKVStore() {
    flush_channel_.send(Stop);
    if (state_->memtable_.size_bytes() > 0) {
        auto _ = SSTable::from_memtable(state_->memtable_.id(), this->config_.directory_, state_->memtable_.freeze());
    }
}
