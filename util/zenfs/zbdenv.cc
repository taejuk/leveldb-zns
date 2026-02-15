#pragma once
#include "leveldb/zenfs/zbdenv.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <mntent.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <set>
#include <sstream>
#include <utility>
#include <vector>

#include "util/coding.h"
#include "util/crc32c.h"

#define DEFAULT_TAEJUK_LOG_PATH "/tmp/"

namespace leveldb {
Status Superblock::DecodeFrom(Slice* input) {
  if (input->size() != ENCODED_SIZE) {
    return Status::Corruption("ZenFS Superblock",
                              "Error: Superblock size missmatch");
  }

  GetFixed32(input, &magic_);
  memcpy(&uuid_, input->data(), sizeof(uuid_));
  input->remove_prefix(sizeof(uuid_));
  GetFixed32(input, &sequence_);
  GetFixed32(input, &flags_);
  GetFixed32(input, &block_size_);
  GetFixed32(input, &zone_size_);
  GetFixed32(input, &nr_zones_);
  GetFixed32(input, &finish_treshold_);
  memcpy(&aux_fs_path_, input->data(), sizeof(aux_fs_path_));
  input->remove_prefix(sizeof(aux_fs_path_));
  memcpy(&reserved_, input->data(), sizeof(reserved_));
  input->remove_prefix(sizeof(reserved_));
  assert(input->size() == 0);

  if (magic_ != MAGIC)
    return Status::Corruption("ZenFS Superblock", "Error: Magic missmatch");
  return Status::OK();
}

void Superblock::EncodeTo(std::string* output) {
  sequence_++;
  output->clear();
  PutFixed32(output, magic_);
  output->append(uuid_, sizeof(uuid_));
  PutFixed32(output, sequence_);
  PutFixed32(output, flags_);
  PutFixed32(output, block_size_);
  PutFixed32(output, zone_size_);
  PutFixed32(output, nr_zones_);
  PutFixed32(output, finish_treshold_);
  output->append(aux_fs_path_, sizeof(aux_fs_path_));
  output->append(reserved_, sizeof(reserved_));
  assert(output->length() == ENCODED_SIZE);
}

void Superblock::GetReport(std::string* reportString) {
  reportString->append("Magic:\t\t\t\t");
  PutFixed32(reportString, magic_);
  reportString->append("\nUUID:\t\t\t\t");
  reportString->append(uuid_);
  reportString->append("\nSequence Number:\t\t");
  reportString->append(std::to_string(sequence_));
  reportString->append("\nFlags [Decimal]:\t\t");
  reportString->append(std::to_string(flags_));
  reportString->append("\nBlock Size [Bytes]:\t\t");
  reportString->append(std::to_string(block_size_));
  reportString->append("\nZone Size [Blocks]:\t\t");
  reportString->append(std::to_string(zone_size_));
  reportString->append("\nNumber of Zones:\t\t");
  reportString->append(std::to_string(nr_zones_));
  reportString->append("\nFinish Threshold [%]:\t\t");
  reportString->append(std::to_string(finish_treshold_));
  reportString->append("\nGarbage Collection Enabled:\t");
  reportString->append(std::to_string(!!(flags_ & FLAGS_ENABLE_GC)));
  reportString->append("\nAuxiliary FS Path:\t\t");
  reportString->append(aux_fs_path_);
}

Status Superblock::CompatibleWith(ZonedBlockDevice* zbd) {
  if (block_size_ != zbd->GetBlockSize())
    return Status::Corruption("ZenFS Superblock",
                              "Error: block size missmatch");
  if (zone_size_ != (zbd->GetZoneSize() / block_size_))
    return Status::Corruption("ZenFS Superblock", "Error: zone size missmatch");
  if (nr_zones_ > zbd->GetNrZones())
    return Status::Corruption("ZenFS Superblock",
                              "Error: nr of zones missmatch");

  return Status::OK();
}

Status TaejukMetaLog::AddRecord(const Slice& slice) {
  uint32_t record_sz = slice.size();
  const char* data = slice.data();
  size_t phys_sz;
  uint32_t crc = 0;
  char* buffer;
  int ret;
  Status s;

  phys_sz = record_sz + zMetaHeaderSize;

  if (phys_sz % bs_) phys_sz += bs_ - phys_sz % bs_;

  assert(data != nullptr);
  assert((phys_sz % bs_) == 0);

  ret = posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), phys_sz);
  if (ret) return Status::IOError("Failed to allocate memory");

  memset(buffer, 0, phys_sz);

  crc = crc32c::Extend(crc, (const char*)&record_sz, sizeof(uint32_t));
  crc = crc32c::Extend(crc, data, record_sz);
  crc = crc32c::Mask(crc);

  EncodeFixed32(buffer, crc);
  EncodeFixed32(buffer + sizeof(uint32_t), record_sz);
  memcpy(buffer + sizeof(uint32_t) * 2, data, record_sz);

  s = zone_->Append(buffer, phys_sz);

  free(buffer);
  return s;
}

Status TaejukMetaLog::Read(Slice* slice) {
  char* data = (char*)slice->data();
  size_t read = 0;
  size_t to_read = slice->size();
  int ret;

  if (read_pos_ >= zone_->wp_) {
    slice->clear();
    return Status::OK();
  }

  if ((read_pos_ + to_read) > (zone_->start_ + zone_->max_capacity_)) {
    return Status::IOError("Read across zone");
  }

  while (read < to_read) {
    ret = zbd_->Read(data + read, read_pos_, to_read - read, false);

    if (ret == -1 && errno == EINTR) continue;
    if (ret < 0) return Status::IOError("Read failed");

    read += ret;
    read_pos_ += ret;
  }

  return Status::OK();
}

Status TaejukMetaLog::ReadRecord(Slice* record, std::string* scratch) {
  Slice header;
  uint32_t record_sz = 0;
  uint32_t record_crc = 0;
  uint32_t actual_crc;
  IOStatus s;

  scratch->clear();
  record->clear();

  scratch->append(zMetaHeaderSize, 0);
  header = Slice(scratch->c_str(), zMetaHeaderSize);

  s = Read(&header);
  if (!s.ok()) return s;

  if (header.size() == 0) {
    record->clear();
    return IOStatus::OK();
  }

  GetFixed32(&header, &record_crc);
  GetFixed32(&header, &record_sz);

  scratch->clear();
  scratch->append(record_sz, 0);

  *record = Slice(scratch->c_str(), record_sz);
  s = Read(record);
  if (!s.ok()) return s;

  actual_crc = crc32c::Value((const char*)&record_sz, sizeof(uint32_t));
  actual_crc = crc32c::Extend(actual_crc, record->data(), record->size());

  if (actual_crc != crc32c::Unmask(record_crc)) {
    return Status::IOError("Not a valid record");
  }

  if (read_pos_ % bs_) read_pos_ += bs_ - (read_pos_ % bs_);

  return Status::OK();
}

ZonedEnv::ZonedEnv(ZonedBlockDevice* zbd, Env* target = Env::Default()):zbd_(zbd), EnvWrapper(target) {
  next_file_id_ = 1;
  metadata_writer_.zenFS = this;
}

ZonedEnv::~ZonedEnv() {
  Status s;
  if(gc_worker_) {
    run_gc_worker_ = false;
    gc_worker_->join();
  }
  meta_log_.reset(nullptr);
  ClearFiles();
  delete zbd_;
}

Status ZonedEnv::Mount(bool readonly) {
  std::vector<Zone*> metazones = zbd_->GetMetaZones();
  std::vector<std::unique_ptr<Superblock>> valid_superblocks;
  std::vector<std::unique_ptr<TaejukMetaLog>> valid_logs;
  std::vector<Zone*> valid_zones;
  std::vector<std::pair<uint32_t, uint32_t>> seq_map;

  Status s;

  if (metazones.size() < 2) {
    return Status::NotSupported("Need at least two non-offline meta zones to open for write");
  }

  for (const auto z : metazones) {
    std::unique_ptr<TaejukMetaLog> log;
    std::string scratch;
    Slice super_record;

    if (!z->Acquire()) {
      assert(false);
      return Status::IOError("Could not aquire busy flag of zone" + std::to_string(z->GetZoneNr()));
    }

    log.reset(new TaejukMetaLog(zbd_, z));

    if (!log->ReadRecord(&super_record, &scratch).ok()) continue;

    if (super_record.size() == 0) continue;

    std::unique_ptr<Superblock> super_block;
    s = super_block->DecodeFrom(&super_record);
    if (s.ok()) s = super_block->CompatibleWith(zbd_);
    if (!s.ok()) return s;

    seq_map.push_back(std::make_pair(super_block->GetSeq(), seq_map.size()));
    valid_superblocks.push_back(std::move(super_block));
    valid_logs.push_back(std::move(log));
    valid_zones.push_back(z);
  }

  if (!seq_map.size()) return Status::NotFound("No valid superblock found");

  std::sort(seq_map.begin(), seq_map.end(), std::greater<std::pair<uint32_t, uint32_t>>());

  bool recovery_ok = false;
  unsigned int r = 0;

  for (const auto& sm : seq_map) {
    uint32_t i = sm.second;
    std::string scratch;
    std::unique_ptr<TaejukMetaLog> log = std::move(valid_logs[i]);

    s = RecoverFrom(log.get());
    if (!s.ok()) {
      if (s.IsNotFound()) continue;
      return s;
    }

    r = i;
    recovery_ok = true;
    meta_log_ = std::move(log);
    break;
  }

  if(!recovery_ok) return Status::IOError("Failed to mount filesystem");
  superblock_ = std::move(valid_superblocks[r]);
  zbd_->SetFinishTreshold(superblock_->GetFinishTreshold());

  s = target()->CreateDir(superblock_->GetAuxFsPath());
  if (!s.ok()) return s;

  for (const auto& sm : seq_map) {
    uint32_t i = sm.second;
    if (i != r) valid_logs[i].reset();
  }

  if (!readonly) {
    s = Repair();
    if (!s.ok()) return s;
    std::lock_guard<std::mutex> lock(files_mtx_);
    s = RollMetaZoneLocked();
    if (!s.ok()) return s;

    Status status = zbd_->ResetUnusedIOZones();
    if(!status.ok()) return status;
    if (superblock_->IsGCEnabled()) {
      run_gc_worker_ = true;
      gc_worker_.reset(new std::thread(&ZonedEnv::GCWorker, this));
    }
  }

  return Status::OK();
}
Status ZonedEnv::MkFS(std::string aux_fs_path, uint32_t finish_threshold, bool enable_gc){
  std::vector<Zone*> metazones = zbd_->GetMetaZones();
  std::unique_ptr<TaejukMetaLog> log;
  Zone* meta_zone = nullptr;
  std::string aux_fs_path = FormatPathLexically(aux_fs_p);
  Status s;
  if (aux_fs_path.length() > 255) {
    return Status::InvalidArgument(
        "Aux filesystem path must be less than 256 bytes\n");
  }
  ClearFiles();
  Status status  = zbd_->ResetUnusedIOZones();
  if(!status.ok()) return status;

  for (const auto mz : metazones) {
    if(!mz->Acquire()) {
      assert(false);
      return Status::Corruption("Could not aquire busy flag of zone " +
                             std::to_string(mz->GetZoneNr()));
    }

    if (mz->Reset().ok()) {
      if (!meta_zone) meta_zone = mz;
    }

    if (meta_zone != mz) {
      if (!mz->Release()) return Status::Corruption("Could not unset busy flag of zone " + std::to_string(mz->GetZoneNr()));
    }
  }

  if (!meta_zone) return Status::IOError("Not available meta zones\n");

  log.reset(new TaejukMetaLog(zbd_, meta_zone));

  Superblock super(zbd_, aux_fs_path, finish_threshold, enable_gc);
  std::string super_string;
  super.EncodeTo(&super_string);

  s = log->AddRecord(super_string);
  if (!s.ok()) return s;

  s = PersistSnapshot(log.get());
  if (!s.ok()) return Status::IOError("Failed persist snapshot");

  return Status::OK();
}
std::map<std::string, Env::WriteLifeTimeHint> ZonedEnv::GetWriteLifeTimeHints(){
  std::map<std::string, Env::WriteLifeTimeHint> hint_map;

  for (auto it = files_.begin(); it != files_.end(); it++) {
    std::shared_ptr<ZoneFile> zoneFile = it->second;
    std::string filename = it->first;
    hint_map.insert(std::make_pair(filename, zoneFile->GetWriteLifeTimeHint()));
  }

  return hint_map;
}

Status ZonedEnv::NewSequentialFile(const std::string& filename, SequentialFile** result){
  std::string fname = FormatPathLexically(filename);
  std::shared_ptr<ZoneFile> zoneFile = GetFile(fname);

  if (zoneFile == nullptr) return target()->NewSequentialFile(ToAuxPath(fname), result);
  *result = new ZonedSequentialFile(zoneFile);
  return Status::OK();
}
Status ZonedEnv::NewRandomAccessFile(const std::string& filename, RandomAccessFile** result){
  std::string fname = FormatPathLexically(filename);
  std::shared_ptr<ZoneFile> zoneFile = GetFile(fname);

  if (zoneFile == nullptr) return target()->NewRandomAccessFile(ToAuxPath(fname), result);
  *result = new ZonedRandomAccessFile(files_[fname]);
  return Status::OK();
}
Status ZonedEnv::NewWritableFile(const std::string& filename, WritableFile** result,WriteLifeTimeHint hint = WLTH_NOT_SET){
  std::string fname = FormatPathLexically(filename);
  return OpenWritableFile(fname, result, hint, false);
}
Status ZonedEnv::NewAppendableFile(const std::string& filename, WritableFile** result,WriteLifeTimeHint hint = WLTH_NOT_SET){
  std::string fname = FormatPathLexically(filename);
  return OpenWritableFile(fname, result, hint, true);
}

Status ZonedEnv::OpenWritableFile(const std::string& filename, WritableFile** result, WriteLifeTimeHint hint, bool reopen) {
  Status s;
  std::string fname = FormatPathLexically(filename);
  bool resetIOZones = false;
  {
    std::lock_guard<std::mutex> file_lock(files_mtx_);
    std::shared_ptr<ZoneFile> zoneFile = GetFileNoLock(fname);

    if(reopen && zoneFile != nullptr) {
      zoneFile->AcquireWRLock();
      *result = new ZonedWritableFile(zbd_, false, zoneFile);
      return Status::OK();
    }

    if (zoneFile != nullptr) {
      s = DeleteFileNoLock(fname);
      if (!s.ok()) return s;
      resetIOZones = true;
    }

    zoneFile = std::make_shared<ZoneFile>(zbd_, next_file_id_++, &metadata_writer_);
    zoneFile->SetFileModificationTime(time(0));
    zoneFile->AddLinkName(fname);

    if (hint == WLTH_NOT_SET) {
      if (fname.find(".log") != std::string::npos) hint = Env::WLTH_SHORT;
      else hint = Env::WLTH_LONG;
    }
    zoneFile->SetWriteLifeTimeHint(hint);
    s = SyncFileMetadataNoLock(zoneFile);
    if (!s.ok()) {
      zoneFile.reset();
      return s;
    }
    zoneFile->AcquireWRLock();
    files_.insert(std::make_pair(fname.c_str(), zoneFile));
    *result = new ZonedWritableFile(zbd_, true, zoneFile);
  }
  
  if (resetIOZones) s = zbd_->ResetUnusedIOZones();
  return s;
}


bool ZonedEnv::FileExists(const std::string& filename) {
  std::string fname = FormatPathLexically(filename);
  if (GetFile(fname) == nullptr) {
    return target()->FileExists(ToAuxPath(fname));
  } else {
    return Status::OK();
  }
}

Status ZonedEnv::GetChildrenNoLock(const std::string& dir_path, std::vector<std::string>* result) {
  std::vector<std::string> auxfiles;
  std::string dir = FormatPathLexically(dir_path);
  Status s;

  s = target()->GetChildren(ToAuxPath(dir), &auxfiles);
  if (!s.ok()) {
    if(s.IsNotFound()) return Status::OK();
    return s;
  }

  for(const auto& f : auxfiles) {
    if (f != "." && f != "..") result->push_back(f);
  }

  GetTaejukChildrenNoLock(dir, false, result);

  return s;
}

void ZonedEnv::GetTaejukChildrenNoLock(const std::string& dir, bool include_grandchildren, std::vector<std::string>* result){
  auto path_as_string_with_separator_at_end = [](fs::path const& path) {
    fs::path with_sep = path / fs::path("");
    return with_sep.lexically_normal().string();
  };

  auto string_starts_with = [](std::string const& string,
                               std::string const& needle) {
    return string.rfind(needle, 0) == 0;
  };

  std::string dir_with_terminating_seperator =
      path_as_string_with_separator_at_end(fs::path(dir));

  auto relative_child_path =
      [&dir_with_terminating_seperator](std::string const& full_path) {
        return full_path.substr(dir_with_terminating_seperator.length());
      };

  for (auto const& it : files_) {
    fs::path file_path(it.first);
    assert(file_path.has_filename());

    std::string file_dir =
        path_as_string_with_separator_at_end(file_path.parent_path());

    if (string_starts_with(file_dir, dir_with_terminating_seperator)) {
      if (include_grandchildren ||
          file_dir.length() == dir_with_terminating_seperator.length()) {
        result->push_back(relative_child_path(file_path.string()));
      }
    }
  }
}

Status ZonedEnv::GetChildren(const std::string& dir, std::vector<std::string>* result) {
  std::lock_guard<std::mutex> lock(files_mtx_);
  return GetChildrenNoLock(dir, result);
}



Status ZonedEnv::DeleteFile(const std::string& fname) {
  Status s;
  files_mtx_.lock();
  s = DeleteFileNoLock(fname);
  files_mtx_.unlock();
  if (s.ok()) s = zbd_->ResetUnusedIOZones();
  return s;
}
Status ZonedEnv::GetFileSize(const std::string& filename, uint64_t* file_size) {
  std::shared_ptr<ZoneFile> zoneFile(nullptr);
  std::string f = FormatPathLexically(filename);
  Status s;

  std::lock_guard<std::mutex> lock(files_mtx_);
  if (files_.find(f) != files_.end()) {
    zoneFile = files_[f];
    *file_size = zoneFile->GetFileSize();
  } else {
    s = target()->GetFileSize(ToAuxPath(f), file_size);
  }

  return s;
}
Status ZonedEnv::RenameFile(const std::string& src, const std::string& target){
  Status s;
  {
    std::lock_guard<std::mutex> lock(files_mtx_);
    s = RenameFileNoLock(src, target);
  }
  if (s.ok()) s = zbd_->ResetUnusedIOZones();
  return s; 
}

Status ZonedEnv::DeleteDir(const std::string& d) {
  std::vector<std::string> children;
  Status s;

  s = GetChildren(d, &children);
  if (children.size() != 0) return Status::IOError("Directory has children");
  return target()->DeleteDir(ToAuxPath(d));
}
Status ZonedEnv::LockFile(const std::string& fname, FileLock** lock) {
  return target()->LockFile(ToAuxPath(fname), lock);
}
Status ZonedEnv::UnlockFile(FileLock* lock) {
  return target()->UnlockFile(lock);
}
Status ZonedEnv::GetTestDirectory(std::string* path) {
  return Status::OK();
}
Status ZonedEnv::NewLogger(const std::string& fname, Logger** result) {
  return Status::OK();
}

Status ZonedEnv::SyncFileMetadata(ZoneFile* zoneFile, bool replace = false) {
  std::lock_guard<std::mutex> lock(files_mtx_);
  return SyncFileMetadataNoLock(zoneFile, replace);
}

Status ZonedEnv::SyncFileMetadataNoLock(ZoneFile* zoneFile, bool replace = false) {
  std::string fileRecord;
  std::string output;
  Status s;

  if(zoneFile->IsDeleted()) return Status::OK();
  if(replace) {
    PutFixed32(&output, kFileReplace);
  } else {
    zoneFile->SetFileModificationTime(time(0));
    PutFixed32(&output, kFileUpdate);
  }
  zoneFile->EncodeUpdateTo(&fileRecord);
  PutLengthPrefixedSlice(&output, Slice(fileRecord));

  s = PersistRecord(output);
  if (s.ok()) zoneFile->MetadataSynced();

  return s;
}

void ZonedEnv::GetTaejukSnapshot(TaejukSnapshot& snapshot, const TaejukSnapshotOptions& options){
  if (options.zbd_) {
    snapshot.zbd_ = ZBDSnapshot(*zbd_);
  }
  if (options.zone_) {
    zbd_->GetZoneSnapshot(snapshot.zones_);
  }
  if (options.zone_file_) {
    std::lock_guard<std::mutex> file_lock(files_mtx_);
    for (const auto& file_it : files_) {
      ZoneFile& file = *(file_it.second);

      if (!file.TryAcquireWRLock()) continue;

      snapshot.zone_files_.emplace_back(file);
      for (auto* ext : file.GetExtents()) {
        snapshot.extents_.emplace_back(*ext, file.GetFilename());
      }

      file.ReleaseWRLock();
    }
  }
}
Status ZonedEnv::MigrateExtents(const std::vector<ZoneExtentSnapshot*>& extents){
  Status s;
  std::map<std::string, std::vector<ZoneExtentSnapshot*>> file_extents;
  for (auto *ext : extents) {
    std::string fname = ext->filename;

    if (ends_with(fname, ".sst")) {
      file_extents[fname].emplace_back(ext);
    }
  }

  for (const auto& it : file_extents) {
    s = MigrateFileExtents(it.first, it.second);
    if(!s.ok()) break;
    s = zbd_->ResetUnusedIOZones();
    if (!s.ok()) break;
  }
  return s;
}
Status ZonedEnv::MigrateFileExtents(const std::string& fname, const std::vector<ZoneExtentSnapshot*>& migrate_exts){
  Status s = Status::OK();
  auto zfile = GetFile(fname);
  
}

std::shared_ptr<ZoneFile> ZonedEnv::GetFileNoLock(const std::string fname){}
std::shared_ptr<ZoneFile> ZonedEnv::GetFile(std::string fname){}
Status ZonedEnv::DeleteFileNoLock(const std::string& fname){}
Status ZonedEnv::SyncFileMetadataNoLock(ZoneFile* zoneFile, bool replace = false){}
Status ZonedEnv::RenameFileNoLock(const std::string& src_path, const std::string& dst_path){
  std::shared_ptr<ZoneFile> source_file(nullptr);
  std::shared_ptr<ZoneFile> existing_dest_file(nullptr);
  std::string source_path = FormatPathLexically(src_path);
  std::string dest_path = FormatPathLexically(dst_path);
  Status s;

  source_file = GetFileNoLock(source_path);
  if (source_file != nullptr) {
    existing_dest_file = GetFileNoLock(dest_path);
    if (existing_dest_file != nullptr) {
      s = DeleteFileNoLock(dest_path);
      if (!s.ok()) { return s; }
    }
    
    s = source_file->RenameLink(source_path, dest_path);
    if (!s.ok()) return s;
    files_.erase(source_path);

    files_.insert(std::make_pair(dest_path, source_file));

    s = SyncFileMetadataNoLock(source_file);
    if (!s.ok()) {
      /* Failed to persist the rename, roll back */
      files_.erase(dest_path);
      s = source_file->RenameLink(dest_path, source_path);
      if (!s.ok()) return s;
      files_.insert(std::make_pair(source_path, source_file));
    }
  } else {
    s = RenameAuxPathNoLock(source_path, dest_path);
  }

  return s;
}

Status ZonedEnv::WriteSnapshotLocked(TaejukMetaLog* meta_log){}
Status ZonedEnv::WriteEndRecord(TaejukMetaLog* meta_log){}
Status ZonedEnv::RollMetaZoneLocked(){}
Status ZonedEnv::PersistSnapshot(TaejukMetaLog* meta_writer){}
Status ZonedEnv::PersistRecord(std::string record){}

void ZonedEnv::EncodeSnapshotTo(std::string* output){}
void ZonedEnv::EncodeFileDeletionTo(ZoneFile* zoneFile, std::string* output, std::string linkf){}

Status ZonedEnv::DecodeSnapshotFrom(Slice* input){}
Status ZonedEnv::DecodeFileUpdateFrom(Slice* slice, bool replace = false){}
Status ZonedEnv::DecodeFileDeletionFrom(Slice* slice){}
Status ZonedEnv::RecoverFrom(TaejukMetaLog* log){}

std::string ZonedEnv::FormatPathLexically(fs::path filepath);

Status NewZonedEnv(Env** env, const std::string& bdevname) {}

}

inline bool ends_with(std::string const& value, std::string const& ending) {
  if (ending.size() > value.size()) return false;
  return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}
