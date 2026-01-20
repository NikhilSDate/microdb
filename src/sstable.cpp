#include "include/sstable.hpp"
#include <format>
#include <fstream>
#include <stdexcept>
#include <filesystem>
#include <span>
#include <cstring>
#include <map>
#include <string>
#include <assert.h>
#include <utility>

struct Test {
    int& i;
};

// File class implementation
File File::open(std::filesystem::path path) {
    File file;
    file.f.open(path, std::ios::in | std::ios::out | std::ios::binary);
    if (!file.f.is_open()) {
        throw std::runtime_error("Failed to open file: " + path.string());
    }
    return file;
}

File File::create(std::filesystem::path path, const std::span<std::byte> data) {
    File file;
    file.f.open(path, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
    if (!file.f.is_open()) {
        throw std::runtime_error("Failed to create file: " + path.string());
    }

    // Write the data to the file
    file.f.write(reinterpret_cast<const char*>(data.data()), data.size());
    file.f.flush();

    // Reset to beginning
    file.f.seekg(0);

    return file;
}

void File::read(std::filesystem::path, std::span<std::byte> buf, size_t offset, size_t len) {
    if (!f.is_open()) {
        throw std::runtime_error("File is not open");
    }

    if (len > buf.size()) {
        throw std::invalid_argument("Buffer too small for requested read length");
    }

    // Seek to offset
    f.seekg(offset);

    // Read len bytes into buffer
    f.read(reinterpret_cast<char*>(buf.data()), len);

    if (f.fail() && !f.eof()) {
        throw std::runtime_error("Failed to read from file");
    }
}

// SSTable class implementation
SSTable SSTable::from_memtable(size_t id, std::filesystem::path directory, const std::map<std::string, std::string>& memtable) {
    SSTable sstable;
    auto filename = std::format("sstable-{0}.sst", id);
    std::filesystem::path file_path = directory.append(filename);
    
    std::vector<std::byte> file_contents;
    for (auto const& [k, v]: memtable) {
        for (auto const& c: k) {
            file_contents.push_back(static_cast<std::byte>(c));
        }
        for (auto const& c: k) {
            file_contents.push_back(static_cast<std::byte>(c));
        }
    }
    size_t offsets_start = file_contents.size();
    Offsets offsets = Offsets::from_memtable(memtable);
    SparseIndex index = SparseIndex::from_memtable_and_offsets(memtable, offsets);

    auto encoded_offsets = offsets.to_raw();
    auto encoded_index = index.to_raw();
    file_contents.insert(file_contents.end(), encoded_offsets.begin(), encoded_offsets.end());
    file_contents.insert(file_contents.end(), encoded_index.begin(), encoded_index.end());
    FileIndex file_index = {.index_start_ = offsets_start + encoded_offsets.size(), .offsets_start_ = offsets_start, };
    auto encoded_file_index = file_index.to_raw();
    file_contents.insert(file_contents.end(), encoded_file_index.begin(), encoded_file_index.end());
    sstable.file = File::create(file_path, file_contents);

    return sstable;
}

std::optional<std::string> SSTable::get(std::string) {
    // first check the 
    return std::nullopt;
}

// FileIndex implementation
FileIndex FileIndex::from_raw(std::span<std::byte>) {
    FileIndex file_index;
    file_index.index_start_ = 0;
    file_index.offsets_start_ = 0;
    return file_index;
}

std::vector<std::byte> FileIndex::to_raw() const {
    std::vector<std::byte> result;
    result.reserve(3 * sizeof(size_t));

    auto index_bytes = std::as_bytes(std::span{&index_start_, 1});
    result.insert(result.end(), index_bytes.begin(), index_bytes.end());

    auto offsets_bytes = std::as_bytes(std::span{&offsets_start_, 1});
    result.insert(result.end(), offsets_bytes.begin(), offsets_bytes.end());

    auto id_bytes = std::as_bytes(std::span{&id_, 1});
    result.insert(result.end(), id_bytes.begin(), id_bytes.end());

    return result;
}

// Offsets implementation
Offsets Offsets::from_raw(std::span<std::byte> raw) {
    assert(raw.size() % (2 * sizeof(size_t)) == 0);
    constexpr size_t entry_size = (2 * sizeof(size_t));
    Offsets offsets;
    size_t num_entries = raw.size() / entry_size;
    for (size_t i = 0; i < num_entries; i++) {
        auto entry = reinterpret_cast<size_t*>(raw.subspan(i * entry_size, entry_size).data());
        auto k = entry[0];
        auto v = entry[1];
        offsets.key_offsets_.push_back(k);
        offsets.value_offsets_.push_back(v);
    }
    return offsets;
}

std::vector<std::byte> Offsets::to_raw() const {
    std::vector<std::byte> result;
    result.reserve(this->key_offsets_.size() * 2 * sizeof(size_t));
    
    assert(this->key_offsets_.size() == this->value_offsets_.size());
    for (size_t i = 0; i < this->key_offsets_.size(); i++) {
        auto key_offset = key_offsets_[i];
        auto value_offset = value_offsets_[i];
        
        auto key_bytes = std::as_bytes(std::span{&key_offset, 1});
        result.insert(result.end(), key_bytes.begin(), key_bytes.end());
        
        auto value_bytes = std::as_bytes(std::span{&value_offset, 1});
        result.insert(result.end(), value_bytes.begin(), value_bytes.end());
    }
    return result;
}

Offsets Offsets::from_memtable(const std::map<std::string, std::string>& memtable) {
    Offsets offsets;
    size_t entry_offset = 0;
    // iteration order is deterministic for map, so this will be consistent with the order earlier
    for (auto const& [k, v]: memtable) {
        offsets.key_offsets_.push_back(entry_offset);
        offsets.value_offsets_.push_back(entry_offset + k.size());
        entry_offset += (k.size() + v.size());
    }
    return offsets;
}   

std::pair<size_t, size_t> Offsets::at(size_t index) const {
    return std::make_pair(key_offsets_.at(index), value_offsets_.at(index));
}

// SparseIndex implementation
SparseIndex SparseIndex::from_raw(std::span<std::byte>) {

    SparseIndex sparse_index;
    return sparse_index;
}

SparseIndex SparseIndex::from_memtable_and_offsets(const std::map<std::string, std::string> &memtable, const Offsets& offsets) {
    SparseIndex index;
    assert(memtable.size() > 0);
    size_t idx = 0;
    auto first_key = (*memtable.begin()).first;
    index.index_[first_key] = idx;
    for (auto const& [k, v]: memtable) {
        // TODO: fix this
        if (offsets.at(idx).first > 4096) {
            index.index_[k] = idx;
        }
        idx += 1;
    }
    return index;
}

std::vector<std::byte> SparseIndex::to_raw() const {
    std::vector<std::byte> result;

    // Serialize each entry: [key_length][key_bytes][index_value]
    for (const auto& [key, index] : index_) {
        // Append key length
        size_t key_length = key.size();
        auto length_bytes = std::as_bytes(std::span{&key_length, 1});
        result.insert(result.end(), length_bytes.begin(), length_bytes.end());

        for (char c : key) {
            result.push_back(static_cast<std::byte>(c));
        }

        auto index_bytes = std::as_bytes(std::span{&index, 1});
        result.insert(result.end(), index_bytes.begin(), index_bytes.end());
    }

    return result;
}
