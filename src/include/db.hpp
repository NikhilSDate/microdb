#include <mutex>
#include <shared_mutex>
#include <string>
#include <map>
#include <optional>
#include <variant>
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
        // these methods are NOT thread-safe (to allow MemTable to be copied/moved)
    public:
        MemTable(const MemTable& other); // this is made explicit because it involves taking the lock
        
        // Moves are NOT thread safe on the object being moved from
        MemTable(MemTable&& other);

        MemTable& operator=(MemTable other);
        MemTable(size_t id): id_{id}, size_{0} {};

        size_t id() { return id_; };
        
        std::optional<std::string> get(const std::string& k);
        void put(const std::string& k, const std::string& v);
        size_t size_bytes() { return size_; };
    private:
        size_t id_;
        mutable std::shared_mutex lock_;
        std::map<std::string, std::string> memtable_;
        size_t size_;
};

struct LSMStoreState {    
    public:
        LSMStoreState(): memtable_(0), next_table_id_{1} {};
        LSMStoreState open_dir(std::filesystem::path directory);
        size_t next_table_id() {return next_table_id_++; };
        MemTable memtable_;
        std::vector<MemTable> immutable_memtables_;
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
        Channel<std::monostate> flush_channel_;
        std::shared_mutex snapshot_lock_;
        std::shared_mutex state_lock_;
        std::shared_ptr<LSMStoreState> state_;
};