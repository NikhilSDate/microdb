
#include <assert.h>
#include <cstring>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <map>
#include <memtable.hpp>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

#include "sstable.hpp"
#include "memtable.hpp"

File File::create(std::filesystem::path path, const std::span<std::byte> data) {
  File file;
  file.path_ = path;
  file.f_.open(path);
  if (!file.f_.is_open()) {
    throw std::runtime_error("Failed to create file: " + path.string());
  }
  std::fstream write_stream;
  write_stream.open(path);
  // Write the data to the file
  write_stream.write(reinterpret_cast<const char *>(data.data()), data.size());
  write_stream.flush();

  // Reset to beginning
  file.f_.seekg(0);

  return file;
}

File File::open(std::filesystem::path path) {
  File file;
  file.path_ = path;
  file.f_.open(path, std::ios::in | std::ios::binary);
  if (!file.f_.is_open()) {
    throw std::runtime_error("Failed to open file: " + path.string());
  }
  return file;
}

File::File(const File& other): path_{other.path_} {
  f_.open(path_);
  if (!f_.is_open()) {
    throw std::runtime_error("Failed to open file: " + path_.string());
  }
}

File& File::operator=(File other) {
  std::swap(this->path_, other.path_);
  std::swap(this->f_, other.f_);
  return *this;
}

File::File(File&& other): File() {
  std::swap(this->path_, other.path_);
  std::swap(this->f_, other.f_);
}

size_t File::size() const {
  auto f_mut = const_cast<std::ifstream *>(&f_);
  auto current_pos = f_mut->tellg();
  f_mut->seekg(0, std::ios::end);
  auto file_size = static_cast<size_t>(f_mut->tellg());
  f_mut->seekg(current_pos);
  return file_size;
}

void File::read(std::span<std::byte> buf, size_t offset, size_t len) {
  if (!f_.is_open()) {
    throw std::runtime_error("File is not open");
  }

  if (len > buf.size()) {
    throw std::invalid_argument("Buffer too small for requested read length");
  }
  auto f_mut = const_cast<std::ifstream *>(&f_);
  // Seek to offset
  f_mut->seekg(offset);

  // Read len bytes into buffer
  f_mut->read(reinterpret_cast<char *>(buf.data()), len);

  if (f_mut->fail() && !f_mut->eof()) {
    throw std::runtime_error("Failed to read from file");
  }
}

// SSTable class implementation
SSTable
SSTable::from_memtable(size_t id, std::filesystem::path directory,
                       const MemTable<Immutable>& memtable) {
  SSTable sstable;
  sstable.id_ = id;
  auto filename = std::format("sstable-{0}.sst", id);
  std::filesystem::path file_path = directory.append(filename);

  std::vector<std::byte> file_contents;
  for (auto const &[k, v] : memtable.memtable_) {
    for (auto const &c : k) {
      file_contents.push_back(static_cast<std::byte>(c));
    }
    for (auto const &c : v) {
      file_contents.push_back(static_cast<std::byte>(c));
    }
  }
  size_t offsets_start = file_contents.size();
  Offsets offsets = Offsets::from_memtable(memtable.memtable_);
  SparseIndex index = SparseIndex::from_memtable_and_offsets(memtable.memtable_, offsets);

  auto encoded_offsets = offsets.to_raw();
  auto encoded_index = index.to_raw();
  file_contents.insert(file_contents.end(), encoded_offsets.begin(),
                       encoded_offsets.end());
  file_contents.insert(file_contents.end(), encoded_index.begin(),
                       encoded_index.end());
  FileIndex file_index = {.index_start_ =
                              offsets_start + encoded_offsets.size(),
                          .offsets_start_ = offsets_start,
                          .id_ = id};
  auto encoded_file_index = file_index.to_raw();
  file_contents.insert(file_contents.end(), encoded_file_index.begin(),
                       encoded_file_index.end());
  sstable.file = File::create(file_path, file_contents);

  return sstable;
}

SSTable SSTable::from_file(std::filesystem::path filepath) {
  SSTable sstable;
  sstable.file = File::open(filepath);

  // Read FileIndex from the end of the file (3 size_t values)
  constexpr size_t file_index_size = 3 * sizeof(size_t);
  auto file_size = sstable.file.size();
  std::vector<std::byte> file_index_bytes(file_index_size);
  sstable.file.read(file_index_bytes, file_size - file_index_size,
                    file_index_size);
  sstable.file_index_ = FileIndex::from_raw(file_index_bytes);
  sstable.id_ = sstable.file_index_.id_;

  // Read Offsets
  size_t offsets_size =
      sstable.file_index_.index_start_ - sstable.file_index_.offsets_start_;
  std::vector<std::byte> offsets_bytes(offsets_size);
  sstable.file.read(offsets_bytes, sstable.file_index_.offsets_start_,
                    offsets_size);
  sstable.offsets_ = Offsets::from_raw(offsets_bytes);

  // Read SparseIndex
  size_t index_size =
      file_size - file_index_size - sstable.file_index_.index_start_;
  std::vector<std::byte> index_bytes(index_size);
  sstable.file.read(index_bytes, sstable.file_index_.index_start_, index_size);
  sstable.sparse_index_ = SparseIndex::from_raw(index_bytes);

  return sstable;
}

std::optional<std::string> SSTable::get(std::string key) {
  // figure out range to scan from the index
  auto search_range = sparse_index_.lookup_key(key);
  auto start_index = search_range.first;
  auto start_offset = offsets_.at(start_index).first;
  auto end_index = search_range.second.value_or(offsets_.num_entries());
  auto end_offset = search_range.second
                        .and_then([&](auto idx) {
                          return std::make_optional(offsets_.at(idx).first);
                        })
                        .value_or(file_index_.offsets_start_);
  std::vector<std::byte> bytes(end_offset - start_offset, std::byte(0));
  file.read(bytes, start_offset, end_offset - start_offset);
  // now scan this block to look for the key
  for (auto index = start_index; index < end_index; index++) {
    auto key_offset = offsets_.at(index).first - start_offset;
    auto value_offset = offsets_.at(index).second - start_offset;
    // need to special case if we are dealing with the last key
    auto next_key_offset = (index + 1 == end_index)
                               ? bytes.size()
                               : offsets_.at(index + 1).first - start_offset;
    auto key_len = value_offset - key_offset;
    auto value_len = next_key_offset - value_offset;
    auto k = std::string(
        reinterpret_cast<const char *>(bytes.data()) + key_offset, key_len);
    auto v = std::string(
        reinterpret_cast<const char *>(bytes.data()) + value_offset, value_len);

    if (key == k) {
      return std::make_optional(v);
    }
  }
  return std::nullopt;
}

// FileIndex implementation
FileIndex FileIndex::from_raw(std::span<std::byte> raw) {
  assert(raw.size() == 3 * sizeof(size_t));
  FileIndex file_index;
  auto data = reinterpret_cast<const size_t *>(raw.data());
  file_index.index_start_ = data[0];
  file_index.offsets_start_ = data[1];
  file_index.id_ = data[2];
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
    auto entry = reinterpret_cast<size_t *>(
        raw.subspan(i * entry_size, entry_size).data());
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

Offsets
Offsets::from_memtable(const std::map<std::string, std::string> &memtable) {
  Offsets offsets;
  size_t entry_offset = 0;
  // iteration order is deterministic for map, so this will be consistent with
  // the order earlier
  for (auto const &[k, v] : memtable) {
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
SparseIndex SparseIndex::from_raw(std::span<std::byte> raw) {
  SparseIndex sparse_index;
  size_t offset = 0;
  while (offset < raw.size()) {
    // Read key length
    auto key_length =
        *reinterpret_cast<const size_t *>(raw.subspan(offset).data());
    offset += sizeof(size_t);

    // Read key bytes
    std::string key(reinterpret_cast<const char *>(raw.subspan(offset).data()),
                    key_length);
    offset += key_length;

    // Read index value
    auto index_value =
        *reinterpret_cast<const size_t *>(raw.subspan(offset).data());
    offset += sizeof(size_t);

    sparse_index.index_[key] = index_value;
  }
  return sparse_index;
}

SparseIndex SparseIndex::from_memtable_and_offsets(
    const std::map<std::string, std::string> &memtable,
    const Offsets &offsets) {
  SparseIndex index;
  if (memtable.size() == 0) {
    return index;
  }
  size_t idx = 0;
  auto first_key = (*memtable.begin()).first;
  index.index_[first_key] = idx;
  for (auto const &[k, v] : memtable) {
    // TODO: fix this
    if (offsets.at(idx).first > 4096) {
      index.index_[k] = idx;
    }
    idx += 1;
  }
  return index;
}

std::pair<std::size_t, std::optional<std::size_t>>
SparseIndex::lookup_key(std::string key) const {
  auto it = index_.upper_bound(key);
  auto end_index =
      it == index_.end() ? std::nullopt : std::make_optional(it->second);
  size_t start_index = 0;
  if (it != index_.begin()) {
    --it;
    start_index = it->second;
  }

  return {start_index, end_index};
}

std::vector<std::byte> SparseIndex::to_raw() const {
  std::vector<std::byte> result;

  // Serialize each entry: [key_length][key_bytes][index_value]
  for (const auto &[key, index] : index_) {
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
