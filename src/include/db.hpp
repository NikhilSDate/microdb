#include <deque>
#include <memory>
#include <queue>
#include <shared_mutex>
#include <string>
#include <optional>
#include <thread>
#include <variant>
#include <vector>
#include <filesystem>
#include "utils.hpp"

#include "iterator.hpp"
#include "memtable.hpp"
#include "sstable.hpp"

enum FlushMessage {
    Flush, 
    Stop
};

struct KVStoreConfig {
    size_t memtable_threshold_;
    std::filesystem::path directory_;

    KVStoreConfig() = delete;
    KVStoreConfig(size_t memtable_threshold, std::filesystem::path directory): memtable_threshold_(memtable_threshold), directory_(directory) {

    }
};

struct LSMStoreState {
    public:
        static LSMStoreState fresh(std::filesystem::path directory);
        static LSMStoreState open_dir(std::filesystem::path directory);
        size_t next_table_id() {return next_table_id_++; };
        MemTable<Mutable> memtable_;
        std::deque<std::shared_ptr<MemTable<Immutable>>> immutable_memtables_;
        std::map<size_t, std::shared_ptr<SSTable>> sstables_;
    private:
        LSMStoreState(MemTable<Mutable> memtable, size_t next_table_id)
            : memtable_(std::move(memtable)), next_table_id_(next_table_id) {}
        size_t next_table_id_;
};

class LSMKVStore {
    public:
        LSMKVStore(const KVStoreConfig& config);
        std::optional<std::string> get(std::string k);
        void put(std::string k, std::string v);
        void remove(std::string k);
        std::vector<std::pair<std::string, std::string>> scan(
            std::optional<std::string> lower = std::nullopt, std::optional<std::string> upper = std::nullopt);
        ~LSMKVStore();
    private:
        void put_internal(std::string k, std::string v);

        std::string directory_;
        KVStoreConfig config_;
        Channel<FlushMessage> flush_channel_;
        std::jthread flush_thead_;
        std::shared_mutex snapshot_lock_;
        std::shared_mutex state_lock_;
        std::shared_ptr<LSMStoreState> state_;
    
    friend void flush_thread_func(LSMKVStore& store);
};

