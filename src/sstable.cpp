#include <algorithm>
#include <assert.h>
#include <filesystem>
#include <format>
#include <fstream>
#include <logging.hpp>
#include <memtable.hpp>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>

#include "sstable.hpp"
#include "memtable.hpp"

File File::create(std::filesystem::path path, const std::span<std::byte> data) {
  std::fstream write_stream;
  write_stream.open(path, std::ios::out | std::ios::trunc);
  if (!write_stream.is_open()) {
    throw std::runtime_error(std::format("Failed to create file {0} for writing", path.string()));
  }
  write_stream.write(reinterpret_cast<const char *>(data.data()), data.size());
  write_stream.flush();
  return File::open(path);
}

File File::open(std::filesystem::path path) {
  File file;
  file.path_ = path;
  file.f_.open(path, std::ios::in);
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
  f_mut->seekg(offset);
  f_mut->read(reinterpret_cast<char *>(buf.data()), len);
  if (f_mut->fail() && !f_mut->eof()) {
    throw std::runtime_error("Failed to read from file");
  }
}

Block Block::from_raw(std::span<std::byte> raw) {
  Block b;
  b.data_.assign(raw.begin(), raw.end());
  return b;
}

std::optional<std::string> Block::get(const std::string& key) const {
  size_t offset = 0;
  while (offset + 8 <= data_.size()) {
    uint32_t key_len = *reinterpret_cast<const uint32_t *>(data_.data() + offset);
    offset += 4;
    if (key_len == 0) break;
    if (offset + key_len + 4 > data_.size()) break;
    std::string k(reinterpret_cast<const char *>(data_.data() + offset), key_len);
    offset += key_len;
    uint32_t val_len = *reinterpret_cast<const uint32_t *>(data_.data() + offset);
    offset += 4;
    if (offset + val_len > data_.size()) break;
    std::string v(reinterpret_cast<const char *>(data_.data() + offset), val_len);
    offset += val_len;
    if (k == key) return v;
  }
  return std::nullopt;
}

// BlockBuilder implementation

bool BlockBuilder::push(const std::string& key, const std::string& value) {
  uint32_t key_len = static_cast<uint32_t>(key.size());
  uint32_t val_len = static_cast<uint32_t>(value.size());
  size_t needed = 4 + key_len + 4 + val_len;
  if (!data_.empty() && data_.size() + needed > BLOCK_SIZE) {
    return false;
  }
  auto *kp = reinterpret_cast<const std::byte *>(&key_len);
  data_.insert(data_.end(), kp, kp + 4);
  for (char c : key) data_.push_back(static_cast<std::byte>(c));
  auto *vp = reinterpret_cast<const std::byte *>(&val_len);
  data_.insert(data_.end(), vp, vp + 4);
  for (char c : value) data_.push_back(static_cast<std::byte>(c));
  return true;
}

std::vector<std::byte> BlockBuilder::build() {
  data_.resize(BLOCK_SIZE, std::byte(0));
  std::vector<std::byte> result;
  std::swap(result, data_);
  return result;
}

// Metadata implementation

Metadata Metadata::from_raw(std::span<std::byte> raw) {
  Metadata m;
  size_t offset = 0;
  while (offset + 4 <= raw.size()) {
    uint32_t key_len = *reinterpret_cast<const uint32_t *>(raw.data() + offset);
    offset += 4;
    if (offset + key_len > raw.size()) break;
    std::string key(reinterpret_cast<const char *>(raw.data() + offset), key_len);
    offset += key_len;
    m.first_keys_.push_back(key);
  }
  return m;
}

std::vector<std::byte> Metadata::to_raw() const {
  std::vector<std::byte> result;
  for (const auto& key : first_keys_) {
    uint32_t key_len = static_cast<uint32_t>(key.size());
    auto *p = reinterpret_cast<const std::byte *>(&key_len);
    result.insert(result.end(), p, p + 4);
    for (char c : key) result.push_back(static_cast<std::byte>(c));
  }
  return result;
}

size_t Metadata::lookup_block(const std::string& key) const {
  if (first_keys_.empty()) return 0;
  auto it = std::upper_bound(first_keys_.begin(), first_keys_.end(), key);
  if (it == first_keys_.begin()) return 0;
  --it;
  return static_cast<size_t>(it - first_keys_.begin());
}

FileIndex FileIndex::from_raw(std::span<std::byte> raw) {
  assert(raw.size() == FileIndex::SIZE);
  FileIndex fi;
  size_t offset = 0;
  fi.block_size = *reinterpret_cast<const uint16_t *>(raw.data() + offset);
  offset += sizeof(uint16_t);
  fi.num_blocks = *reinterpret_cast<const uint32_t *>(raw.data() + offset);
  offset += sizeof(uint32_t);
  fi.id = *reinterpret_cast<const size_t *>(raw.data() + offset);
  return fi;
}

std::vector<std::byte> FileIndex::to_raw() const {
  std::vector<std::byte> result(SIZE);
  size_t offset = 0;
  *reinterpret_cast<uint16_t *>(result.data() + offset) = block_size;
  offset += sizeof(uint16_t);
  *reinterpret_cast<uint32_t *>(result.data() + offset) = num_blocks;
  offset += sizeof(uint32_t);
  *reinterpret_cast<size_t *>(result.data() + offset) = id;
  return result;
}

SSTable SSTable::from_memtable(size_t id, std::filesystem::path directory,
                               const MemTable<Immutable>& memtable) {
  logging::log(std::format("Creating SSTable with id {0}", id));
  SSTable sstable;
  sstable.id_ = id;
  auto filename = std::format("sstable-{0}.sst", id);
  auto file_path = directory / filename;

  std::vector<std::byte> file_contents;
  Metadata metadata;
  BlockBuilder builder;

  for (auto const& [k, v] : memtable.memtable_) {
    bool was_empty = builder.empty();
    if (!builder.push(k, v)) {
      // Block is full: flush and start a new one
      auto block = builder.build();
      file_contents.insert(file_contents.end(), block.begin(), block.end());
      metadata.add_first_key(k);
      builder.push(k, v); // guaranteed to fit in a fresh block
    } else if (was_empty) {
      metadata.add_first_key(k);
    }
  }

  if (!builder.empty()) {
    auto block = builder.build();
    file_contents.insert(file_contents.end(), block.begin(), block.end());
  }

  uint32_t num_blocks = static_cast<uint32_t>(file_contents.size() / BLOCK_SIZE);

  auto meta_raw = metadata.to_raw();
  file_contents.insert(file_contents.end(), meta_raw.begin(), meta_raw.end());

  FileIndex fi;
  fi.block_size = static_cast<uint16_t>(BLOCK_SIZE);
  fi.num_blocks = num_blocks;
  fi.id = id;
  auto fi_raw = fi.to_raw();
  file_contents.insert(file_contents.end(), fi_raw.begin(), fi_raw.end());

  sstable.file_index_ = fi;
  sstable.metadata_ = metadata;
  sstable.file_ = File::create(file_path, file_contents);
  return sstable;
}

SSTable SSTable::from_file(std::filesystem::path filepath) {
  SSTable sstable;
  sstable.file_ = File::open(filepath);

  auto file_size = sstable.file_.size();

  // Read FileIndex from end
  std::vector<std::byte> fi_bytes(FileIndex::SIZE);
  sstable.file_.read(fi_bytes, file_size - FileIndex::SIZE, FileIndex::SIZE);
  sstable.file_index_ = FileIndex::from_raw(fi_bytes);
  sstable.id_ = sstable.file_index_.id;

  // Metadata sits between data blocks and file index
  size_t data_end = static_cast<size_t>(sstable.file_index_.num_blocks) * sstable.file_index_.block_size;
  size_t meta_size = file_size - FileIndex::SIZE - data_end;
  std::vector<std::byte> meta_bytes(meta_size);
  if (meta_size > 0) {
    sstable.file_.read(meta_bytes, data_end, meta_size);
  }
  sstable.metadata_ = Metadata::from_raw(meta_bytes);

  return sstable;
}

std::optional<std::string> SSTable::get(std::string key) {
  if (file_index_.num_blocks == 0) return std::nullopt;

  size_t block_idx = metadata_.lookup_block(key);

  std::vector<std::byte> block_data(file_index_.block_size);
  file_.read(block_data, block_idx * file_index_.block_size, file_index_.block_size);

  Block block = Block::from_raw(block_data);
  return block.get(key);
}
