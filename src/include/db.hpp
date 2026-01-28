#include <shared_mutex>
#include <string>
#include <optional>
#include <thread>
#include <variant>
#include <vector>
#include <filesystem>
#include "utils.hpp"

#include "memtable.hpp"
#include "sstable.hpp"

enum FlushMessage {
    Flush, 
    Stop
};

struct KVStoreConfig {
    size_t memtable_threshold_;
    std::string directory_;

    KVStoreConfig() = delete;
    KVStoreConfig(size_t memtable_threshold, std::string directory): memtable_threshold_(memtable_threshold), directory_(directory) {

    }
};

struct LSMStoreState {
    public:
        LSMStoreState(): memtable_(0), next_table_id_{1} {};
        static LSMStoreState open_dir(std::filesystem::path directory);
        size_t next_table_id() {return next_table_id_++; };
        MemTable<Mutable> memtable_;
        std::vector<MemTable<Immutable>> immutable_memtables_;
        std::map<size_t, SSTable> sstables_;
    private:
        size_t next_table_id_;
};

class LSMKVStore {
    public:
        LSMKVStore(const KVStoreConfig& config);
        std::optional<std::string> get(std::string k);
        void put(std::string k, std::string v);
        void remove(std::string k);
        ~LSMKVStore();
    private:
        std::string directory_;
        KVStoreConfig config_;
        Channel<FlushMessage> flush_channel_;
        std::jthread flush_thead_;
        std::shared_mutex snapshot_lock_;
        std::shared_mutex state_lock_;
        std::shared_ptr<LSMStoreState> state_;
    
    friend void flush_thread_func(LSMKVStore& store);
};

