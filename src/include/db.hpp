#include <mutex>
#include <shared_mutex>
#include <string>
#include <map>
#include <optional>
#include <vector>
#include "sstable.hpp"
#include "utils.hpp"

struct KVStoreConfig {
    size_t memtable_threshold_;
    std::string directory_;

    KVStoreConfig() = delete;
    KVStoreConfig(size_t memtable_threshold, std::string directory): memtable_threshold_(memtable_threshold), directory_(directory) {

    }
};

class MemTable {
        // these methods are thread-safe
    public:
        MemTable(): size_{0} {};
        std::optional<std::string> get(const std::string& k);
        void put(const std::string& k, const std::string& v);
        void size();
    private:
        std::shared_mutex lock_;
        std::map<std::string, std::string> memtable_;
        size_t size_;
};

struct LSMStoreState {
    std::shared_mutex lock; // this is required since we can't guarantee lock-free access to the memtable
    LSMStoreState(): next_sstable_id{0} {};
    MemTable memtable_;
    std::map<size_t, SSTable> sstables_;
    size_t next_sstable_id;
};

class LSMKVStore {
    public:
        LSMKVStore(const KVStoreConfig& config);
        std::optional<std::string> get(std::string k);
        std::string put(std::string k, std::string v);
        void remove(std::string k);
        ~LSMKVStore();
    private:
        std::string directory_;
        KVStoreConfig config_;
        Channel<bool> flush_channel_;
        std::shared_mutex state_lock_;
        std::shared_ptr<LSMStoreState> state_;
};