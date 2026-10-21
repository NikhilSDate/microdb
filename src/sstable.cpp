#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <format>
#include <logging.hpp>
#include <memtable.hpp>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <tuple>
#include <unistd.h>

#include "sstable.hpp"
#include "memtable.hpp"
#include "posix_io.hpp"

using posix_io::throw_errno;

namespace {
template <typename T>
T read_type(const std::byte* p) {
  T value;
  std::memcpy(&value, p, sizeof(T));
  return value;
}

template <typename T>
void write_type(std::byte* p, T value) {
  std::memcpy(p, &value, sizeof(T));
}
}

File File::create(std::filesystem::path path, const std::span<std::byte> data) {
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    throw_errno(std::format("Failed to create file {0} for writing", path.string()));
  }
  try {
    posix_io::write_all(fd, data.data(), data.size(), std::format("Failed to write file {0}", path.string()));
  } catch (...) {
    ::close(fd);
    throw;
  }
  if (::fsync(fd) != 0) {
    ::close(fd);
    throw_errno(std::format("Failed to sync file {0}", path.string()));
  }
  ::close(fd);
  return File::open(path);
}

File File::open(std::filesystem::path path) {
  File file;
  file.path_ = path;
  file.fd_ = ::open(path.c_str(), O_RDONLY);
  if (file.fd_ < 0) {
    throw_errno("Failed to open file: " + path.string());
  }
  struct stat st;
  if (::fstat(file.fd_, &st) != 0) {
    ::close(file.fd_);
    throw_errno("Failed to stat file: " + path.string());
  }
  file.size_ = static_cast<size_t>(st.st_size);
  return file;
}

File::File(const File& other): path_{other.path_}, size_{other.size_} {
  if (other.fd_ < 0) return;
  fd_ = ::dup(other.fd_);
  if (fd_ < 0) {
    throw_errno("Failed to duplicate file descriptor for: " + path_.string());
  }
}

File& File::operator=(File other) {
  std::swap(this->path_, other.path_);
  std::swap(this->fd_, other.fd_);
  std::swap(this->size_, other.size_);
  return *this;
}

File::File(File&& other): File() {
  std::swap(this->path_, other.path_);
  std::swap(this->fd_, other.fd_);
  std::swap(this->size_, other.size_);
}

File::~File() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

void File::read(std::span<std::byte> buf, size_t offset, size_t len) const {
  if (fd_ < 0) {
    throw std::runtime_error("File is not open");
  }
  if (len > buf.size()) {
    throw std::invalid_argument("Buffer too small for requested read length");
  }
  size_t total_read = 0;
  while (total_read < len) {
    ssize_t n = ::pread(fd_, buf.data() + total_read, len - total_read,
                         static_cast<off_t>(offset + total_read));
    if (n < 0) {
      if (errno == EINTR) continue;
      throw_errno("Failed to read from file");
    }
    if (n == 0) break;
    total_read += static_cast<size_t>(n);
  }
}

Block Block::from_raw(std::span<std::byte> raw) {
  Block b;
  b.data_.assign(raw.begin(), raw.end());
  size_t offset = 0;
  while (offset + 8 <= b.data_.size()) {
    uint32_t key_len = read_type<uint32_t>(b.data_.data() + offset);
    if (key_len == 0) break;
    if (offset + 4 + key_len + 4 > b.data_.size()) break;
    b.offsets_.push_back(static_cast<uint32_t>(offset));
    uint32_t val_len = read_type<uint32_t>(b.data_.data() + offset + 4 + key_len);
    offset += 4 + key_len + 4 + val_len;
  }
  return b;
}

std::string_view Block::key_at(size_t idx) const {
  size_t offset = offsets_.at(idx);
  uint32_t key_len = read_type<uint32_t>(data_.data() + offset);
  return std::string_view(reinterpret_cast<const char *>(data_.data() + offset + 4), key_len);
}

std::pair<std::string, std::string> Block::entry_at(size_t idx) const {
  size_t offset = offsets_.at(idx);
  uint32_t key_len = read_type<uint32_t>(data_.data() + offset);
  offset += 4;
  std::string k(reinterpret_cast<const char *>(data_.data() + offset), key_len);
  offset += key_len;
  uint32_t val_len = read_type<uint32_t>(data_.data() + offset);
  offset += 4;
  std::string v(reinterpret_cast<const char *>(data_.data() + offset), val_len);
  return {std::move(k), std::move(v)};
}

namespace {
size_t lower_bound_key(const Block& block, const std::string& key) {
  size_t lo = 0, hi = block.num_entries();
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    if (block.key_at(mid) < key) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}
}

std::optional<std::string> Block::get(const std::string& key) const {
  size_t lo = lower_bound_key(*this, key);
  if (lo == offsets_.size()) return std::nullopt;
  auto [k, v] = entry_at(lo);
  if (k == key) return v;
  return std::nullopt;
}

BlockIterator BlockIterator::seek_to_first(std::shared_ptr<const Block> block) {
  return BlockIterator(std::move(block), 0);
}

BlockIterator BlockIterator::seek_to_key(std::shared_ptr<const Block> block, const std::string& key) {
  size_t idx = lower_bound_key(*block, key);
  return BlockIterator(std::move(block), idx);
}

BlockIterator::BlockIterator(std::shared_ptr<const Block> block, size_t idx)
    : block_(std::move(block)), idx_(idx) {
  load_current();
}

void BlockIterator::load_current() {
  if (idx_ < block_->num_entries()) {
    std::tie(key_, value_) = block_->entry_at(idx_);
  } else {
    key_.clear();
    value_.clear();
  }
}

void BlockIterator::next() {
  idx_++;
  load_current();
}

// BlockBuilder implementation

bool BlockBuilder::push(const std::string& key, const std::string& value) {
  uint32_t key_len = static_cast<uint32_t>(key.size());
  uint32_t val_len = static_cast<uint32_t>(value.size());
  size_t needed = entry_size(key, value);
  if (data_.size() + needed > BLOCK_SIZE) {
    return false;
  }
  size_t entry_start = data_.size();
  data_.resize(entry_start + needed);
  write_type(data_.data() + entry_start, key_len);
  size_t pos = entry_start + 4;
  for (char c : key) data_[pos++] = static_cast<std::byte>(c);
  write_type(data_.data() + pos, val_len);
  pos += 4;
  for (char c : value) data_[pos++] = static_cast<std::byte>(c);
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
    uint32_t key_len = read_type<uint32_t>(raw.data() + offset);
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
    size_t entry_start = result.size();
    result.resize(entry_start + 4 + key_len);
    write_type(result.data() + entry_start, key_len);
    size_t pos = entry_start + 4;
    for (char c : key) result[pos++] = static_cast<std::byte>(c);
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
  fi.block_size = read_type<uint16_t>(raw.data() + offset);
  offset += sizeof(uint16_t);
  fi.num_blocks = read_type<uint32_t>(raw.data() + offset);
  offset += sizeof(uint32_t);
  fi.id = read_type<uint64_t>(raw.data() + offset);
  return fi;
}

std::vector<std::byte> FileIndex::to_raw() const {
  std::vector<std::byte> result(SIZE);
  size_t offset = 0;
  write_type(result.data() + offset, block_size);
  offset += sizeof(uint16_t);
  write_type(result.data() + offset, num_blocks);
  offset += sizeof(uint32_t);
  write_type(result.data() + offset, id);
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
      auto block = builder.build();
      file_contents.insert(file_contents.end(), block.begin(), block.end());
      metadata.add_first_key(k);
      if (!builder.push(k, v)) {
        throw std::runtime_error(std::format(
            "Entry for key '{0}' ({1} bytes) does not fit in a {2}-byte block",
            k, entry_size(k, v), BLOCK_SIZE));
      }
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
  fi.id = static_cast<uint64_t>(id);
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
  sstable.id_ = static_cast<size_t>(sstable.file_index_.id);

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

std::shared_ptr<Block> SSTable::load_block(size_t idx) const {
  std::vector<std::byte> block_data(file_index_.block_size);
  file_.read(block_data, idx * file_index_.block_size, file_index_.block_size);
  return std::make_shared<Block>(Block::from_raw(block_data));
}

std::optional<std::string> SSTable::get(std::string key) {
  if (num_blocks() == 0) return std::nullopt;
  return load_block(find_block(key))->get(key);
}

SSTableIterator SSTableIterator::seek_to_first(std::shared_ptr<SSTable> table) {
  if (table->num_blocks() == 0) return SSTableIterator(std::move(table), 0, std::nullopt);
  auto block = table->load_block(0);
  return SSTableIterator(std::move(table), 0, BlockIterator::seek_to_first(block));
}

SSTableIterator SSTableIterator::seek_to_key(std::shared_ptr<SSTable> table, const std::string& key) {
  if (table->num_blocks() == 0) return SSTableIterator(std::move(table), 0, std::nullopt);
  size_t idx = table->find_block(key);
  auto block = table->load_block(idx);
  SSTableIterator it(table, idx, BlockIterator::seek_to_key(block, key));
  if (!it.block_iter_->is_valid()) it.advance_to_next_block();
  return it;
}

SSTableIterator::SSTableIterator(std::shared_ptr<SSTable> table, size_t block_idx,
                                  std::optional<BlockIterator> block_iter)
    : table_(std::move(table)), block_idx_(block_idx), block_iter_(std::move(block_iter)) {}

void SSTableIterator::advance_to_next_block() {
  block_idx_++;
  if (block_idx_ >= table_->num_blocks()) {
    block_iter_.reset();
    return;
  }
  block_iter_ = BlockIterator::seek_to_first(table_->load_block(block_idx_));
}

void SSTableIterator::next() {
  block_iter_->next();
  if (!block_iter_->is_valid()) advance_to_next_block();
}
