#include <fstream>
#include <string>
#include <optional>
#include <map>
#include <vector>
#include <filesystem>


// file format
// [<key><value><key><value>....][sparse index][offsets: k0v0k1v1...][start of sparse index, start of offsets]

class File {
public:
    static File create(std::filesystem::path, const std::span<std::byte> data);
    static File open(std::filesystem::path path);

    void read(std::span<std::byte> buf, size_t offset, size_t len);
    size_t size() const;

    // Default constructor
    File() = default;

    // Delete copy operations
    File(const File&) = delete;
    File& operator=(const File&) = delete;

    // Default move operations
    File(File&&) = default;
    File& operator=(File&&) = default;

private:
    std::fstream f;
};

struct FileIndex {

    static FileIndex from_raw(std::span<std::byte> raw);
    std::vector<std::byte> to_raw() const;

    size_t index_start_;
    size_t offsets_start_;
    size_t id_;
};

class Offsets {
    public:
        static Offsets from_raw(std::span<std::byte> raw);
        static Offsets from_memtable(const std::map<std::string, std::string>& memtable);
        std::vector<std::byte> to_raw() const;
        std::pair<size_t, size_t> at(size_t idx) const; 
        size_t num_entries() const { return key_offsets_.size(); };
    private:
        std::vector<size_t> key_offsets_;
        std::vector<size_t> value_offsets_;
};

class SparseIndex {
    public:
        static SparseIndex from_raw(std::span<std::byte> raw);
        static SparseIndex from_memtable_and_offsets(const std::map<std::string, std::string>& memtable, const Offsets& offsets);         
        std::vector<std::byte> to_raw() const;
        std::pair<std::size_t, std::optional<std::size_t>> lookup_key(std::string key) const;
    private:
        std::map<std::string, size_t> index_; // this stores mappings from key to index within the file
    };

class SSTable {
public:
    std::optional<std::string> get(std::string k);
    size_t id() const { return id_; }
    static SSTable from_memtable(size_t id, std::filesystem::path directory, const std::map<std::string, std::string>& memtable);
    static SSTable from_file(std::filesystem::path filepath);

    // Default constructor
    SSTable() = default;

    // Delete copy operations
    SSTable(const SSTable&) = delete;
    SSTable& operator=(const SSTable&) = delete;

    // Default move operations
    SSTable(SSTable&&) = default;
    SSTable& operator=(SSTable&&) = default;

private:
    size_t id_;
    File file;
    FileIndex file_index_;
    SparseIndex sparse_index_;
    Offsets offsets_;
};