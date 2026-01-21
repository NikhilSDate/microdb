#include <string>
#include <map>
#include <optional>
#include <vector>
#include <memory>
#include "sstable.hpp"

struct KVStoreConfig {
    size_t memtable_threshold_;
    std::string directory_;

    KVStoreConfig() = delete;
    KVStoreConfig(size_t memtable_threshold, std::string directory): memtable_threshold_(memtable_threshold), directory_(directory) {

    }
};

class LSMKVStore {
    public:
        LSMKVStore(const KVStoreConfig& config);
        std::optional<std::string> get(std::string k);
        std::string set(std::string k, std::string v);
        void remove(std::string k);
        ~LSMKVStore();
    private:
        std::string directory_;
        KVStoreConfig config_;
        std::map<std::string, std::string> memtable_;
        std::vector<SSTable> sstables_;
        size_t next_sstable_id;
};