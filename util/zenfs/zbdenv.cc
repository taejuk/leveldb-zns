#pragma once
#include "leveldb/zenfs/zbdenv.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <mntent.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <iostream>
#include <set>
#include <sstream>
#include <utility>
#include <vector>

#include "util/coding.h"
#include "util/crc32c.h"

#define DEFAULT_TAEJUK_LOG_PATH "/tmp/"

namespace leveldb {
std::atomic<uint64_t> g_zns_user_write_bytes(0);
std::atomic<uint64_t> g_zns_device_write_bytes(0);

inline bool ends_with(std::string const& value, std::string const& ending) {
  if (ending.size() > value.size()) return false;
  return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

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
  std::cout << output->length() << " " << ENCODED_SIZE << std::endl;
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

  //std::cout << read_pos_ << " " << zone_->wp_ << std::endl;
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
  Status s;

  scratch->clear();
  record->clear();

  scratch->append(zMetaHeaderSize, 0);
  header = Slice(scratch->c_str(), zMetaHeaderSize);
  s = Read(&header);
  if (!s.ok()) return s;

  if (header.size() == 0) {
    record->clear();
    return Status::OK();
  }
  bool testret;
  testret = GetFixed32(&header, &record_crc);
  //std::cout << "testret: " << testret << std::endl;
  testret = GetFixed32(&header, &record_sz);
  //std::cout << "testret: " << testret << std::endl;
  scratch->clear();
  scratch->append(record_sz, 0);
  //std::cout << "record sz: " << record_sz << std::endl;
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

ZonedEnv::ZonedEnv(ZonedBlockDevice* zbd, Env* target):zbd_(zbd), EnvWrapper(target) {
  next_file_id_ = 1;
  metadata_writer_.env = this;
}

Status ZonedEnv::Repair() {
  std::map<std::string, std::shared_ptr<ZoneFile>>::iterator it;
  for (it = files_.begin(); it != files_.end(); it++) {
    std::shared_ptr<ZoneFile> zFile = it->second;
    if (zFile->HasActiveExtent()) {
      Status s = zFile->Recover();
      if (!s.ok()) return s;
    }
  }

  return Status::OK();
}

ZonedEnv::~ZonedEnv() {
  Status s;
  if(gc_worker_) {
    run_gc_worker_ = false;
    gc_worker_->join();
  }

  double waf = 0.0;
  if (g_zns_user_write_bytes > 0) {
    waf = (double)g_zns_device_write_bytes / (double)g_zns_user_write_bytes;
  }
  std::cout << "\n========================================" << std::endl;
  std::cout << "       ZNS Write Amplification (WAF)      " << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << " [1] User Writes (LevelDB): " << g_zns_user_write_bytes << " bytes (" 
            << g_zns_user_write_bytes / 1024 / 1024 << " MB)" << std::endl;
  std::cout << " [2] Device Writes (ZNS)  : " << g_zns_device_write_bytes << " bytes (" 
            << g_zns_device_write_bytes / 1024 / 1024 << " MB)" << std::endl;
  std::cout << " ----------------------------------------" << std::endl;
  std::cout << " -> WAF (Device / User)   : " << waf << std::endl;
  std::cout << "========================================\n" << std::endl;
  std::cout << "Free space: " << FreePercent() << "%" << std::endl;
  meta_log_.reset(nullptr);
  ClearFiles();
  delete zbd_;
}

double ZonedEnv::GetWAF() {
  double waf = 0.0;
  if (g_zns_user_write_bytes > 0) {
    waf = (double)g_zns_device_write_bytes / (double)g_zns_user_write_bytes;
  }
  return waf;
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

    super_block.reset(new Superblock());
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

double ZonedEnv::FreePercent() {
  uint64_t non_free = zbd_->GetUsedSpace() + zbd_->GetReclaimableSpace();
  uint64_t free = zbd_->GetFreeSpace();
  return (double)(100 * free) / (double)(free + non_free);
}

void ZonedEnv::ExecuteGC() {
  
  zbd_->ResetUnusedIOZones();

  uint64_t non_free = zbd_->GetUsedSpace() + zbd_->GetReclaimableSpace();
  uint64_t free = zbd_->GetFreeSpace();
  uint64_t free_percent = (100 * free) / (free + non_free);
  TaejukSnapshot snapshot;
  TaejukSnapshotOptions options;

  if (free_percent > GC_START_LEVEL) return;
  options.zone_ = 1;
  options.zone_file_ = 1;
  options.log_garbage_ = 1;

  GetTaejukSnapshot(snapshot, options);

  uint64_t threshold = (100 - GC_SLOPE * (GC_START_LEVEL - free_percent));
  fprintf(stderr, "threshold: %d\n", threshold);
  std::set<uint64_t> migrate_zones_start;
  for (const auto& zone : snapshot.zones_) {
    
    if (zone.capacity == 0) {
      uint64_t garbage_percent_approx =
          100 - 100 * zone.used_capacity / zone.max_capacity;
      //fprintf(stderr, "garbage_percent_approx: %d\n", garbage_percent_approx);
      if (garbage_percent_approx > threshold &&
          garbage_percent_approx < 100) {
        migrate_zones_start.emplace(zone.start);
      }
    }
  }

  std::vector<ZoneExtentSnapshot*> migrate_exts;
  for (auto& ext : snapshot.extents_) {
    if (migrate_zones_start.find(ext.zone_start) !=
        migrate_zones_start.end()) {
      migrate_exts.push_back(&ext);
    }
  }
  std::cout << "Garbage collecting " << (int)migrate_exts.size() << "extents" <<std::endl;
  if (migrate_exts.size() > 0) {
    Status s;
    
    s = MigrateExtents(migrate_exts);
    if (!s.ok()) {
      //Error(logger_, "Garbage collection failed");
    }
  }

}

void ZonedEnv::GCWorker() {
  while (run_gc_worker_) {
    
    usleep(1000 * 1000 * 10);

    uint64_t non_free = zbd_->GetUsedSpace() + zbd_->GetReclaimableSpace();
    uint64_t free = zbd_->GetFreeSpace();
    uint64_t free_percent = (100 * free) / (free + non_free);
    TaejukSnapshot snapshot;
    TaejukSnapshotOptions options;

    if (free_percent > GC_START_LEVEL) continue;
    std::cout << "garbage collection start!" << std::endl;
    options.zone_ = 1;
    options.zone_file_ = 1;
    options.log_garbage_ = 1;

    GetTaejukSnapshot(snapshot, options);

    uint64_t threshold = (100 - GC_SLOPE * (GC_START_LEVEL - free_percent));
    std::set<uint64_t> migrate_zones_start;
    for (const auto& zone : snapshot.zones_) {
      if (zone.capacity == 0) {
        uint64_t garbage_percent_approx =
            100 - 100 * zone.used_capacity / zone.max_capacity;
        if (garbage_percent_approx > threshold &&
            garbage_percent_approx < 100) {
          migrate_zones_start.emplace(zone.start);
        }
      }
    }

    std::vector<ZoneExtentSnapshot*> migrate_exts;
    for (auto& ext : snapshot.extents_) {
      if (migrate_zones_start.find(ext.zone_start) !=
          migrate_zones_start.end()) {
        migrate_exts.push_back(&ext);
      }
    }

    if (migrate_exts.size() > 0) {
      Status s;
      // Info(logger_, "Garbage collecting %d extents \n",
      //      (int)migrate_exts.size());
      s = MigrateExtents(migrate_exts);
      if (!s.ok()) {
        //Error(logger_, "Garbage collection failed");
      }
    }
  }
}
Status ZonedEnv::MkFS(std::string aux_fs_p, uint32_t finish_threshold, bool enable_gc){
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
std::map<std::string, WriteLifeTimeHint> ZonedEnv::GetWriteLifeTimeHints(){
  std::map<std::string, WriteLifeTimeHint> hint_map;

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
Status ZonedEnv::NewWritableFile(const std::string& filename, WritableFile** result,WriteLifeTimeHint hint){
  std::string fname = FormatPathLexically(filename);
  //std::cout << "fname: " << fname << std::endl;
  return OpenWritableFile(fname, result, hint, false);
}
Status ZonedEnv::NewAppendableFile(const std::string& filename, WritableFile** result,WriteLifeTimeHint hint){
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
      if (fname.find(".log") != std::string::npos) hint = WLTH_SHORT;
      else hint = WLTH_LONG;
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
    return true;
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
  std::string filename = fname;
  s = DeleteFileNoLock(filename);
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

void ZonedEnv::ClearFiles() {
  std::map<std::string, std::shared_ptr<ZoneFile>>::iterator it;
  std::lock_guard<std::mutex> file_lock(files_mtx_);
  for (it = files_.begin(); it != files_.end(); it++) it->second.reset();
  files_.clear();
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

Status ZonedEnv::SyncFileMetadata(ZoneFile* zoneFile, bool replace) {
  std::lock_guard<std::mutex> lock(files_mtx_);
  return SyncFileMetadataNoLock(zoneFile, replace);
}

Status ZonedEnv::SyncFileMetadataNoLock(ZoneFile* zoneFile, bool replace) {
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

    if (ends_with(fname, ".sst") || ends_with(fname, ".ldb")) {
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
  if (zfile == nullptr) return Status::OK();

  if (!zfile->TryAcquireWRLock()) return Status::OK();

  std::vector<ZoneExtent*> new_extent_list;
  std::vector<ZoneExtent*> extents = zfile->GetExtents();
  for (const auto* ext : extents) {
    new_extent_list.push_back(new ZoneExtent(ext->start_, ext->length_, ext->zone_));
  }

  for (ZoneExtent* ext : new_extent_list) {
    auto it = std::find_if(migrate_exts.begin(), migrate_exts.end(),
                           [&](const ZoneExtentSnapshot* ext_snapshot) {
                             return ext_snapshot->start == ext->start_ &&
                                    ext_snapshot->length == ext->length_;
                           });
    
    if (it == migrate_exts.end()) continue;

    Zone* target_zone = nullptr;
    
    s = zbd_->TakeMigrateZone(&target_zone, zfile->GetWriteLifeTimeHint(), ext->length_);

    if(!s.ok()) continue;

    if (target_zone == nullptr) {
      zbd_->ReleaseMigrateZone(target_zone);
      continue;
    }

    uint64_t target_start = target_zone->wp_;

    zfile->MigrateData(ext->start_, ext->length_, target_zone);
    zbd_->AddGCBytesWritten(ext->length_);

    if(GetFileNoLock(fname) == nullptr) {
      zbd_->ReleaseMigrateZone(target_zone);
      break;
    }

    ext->start_ = target_start;
    ext->zone_ = target_zone;
    ext->zone_->used_capacity_ += ext->length_;

    zbd_->ReleaseMigrateZone(target_zone);
  }

  SyncFileExtents(zfile.get(), new_extent_list);
  zfile->ReleaseWRLock();

  return Status::OK();
}

Status ZonedEnv::SyncFileExtents(ZoneFile* zoneFile, std::vector<ZoneExtent*> new_extents) {
  Status s;

  std::vector<ZoneExtent*> old_extents = zoneFile->GetExtents();
  zoneFile->ReplaceExtentList(new_extents);
  zoneFile->MetadataUnsynced();
  s = SyncFileMetadata(zoneFile, true);

  if(!s.ok()) return s;

  for (size_t i = 0; i < new_extents.size(); ++i) {
    ZoneExtent* old_ext = old_extents[i];
    if (old_ext->start_ != new_extents[i]->start_) {
      old_ext->zone_->used_capacity_ -= old_ext->length_;
    }
    delete old_ext;
  }

  return Status::OK();
}

std::shared_ptr<ZoneFile> ZonedEnv::GetFileNoLock(std::string fname){
  std::shared_ptr<ZoneFile> zoneFile(nullptr);
  fname = FormatPathLexically(fname);
  if (files_.find(fname) != files_.end()) {
    zoneFile = files_[fname];
  }
  return zoneFile;
}
std::shared_ptr<ZoneFile> ZonedEnv::GetFile(std::string fname){
  std::shared_ptr<ZoneFile> zoneFile(nullptr);
  std::lock_guard<std::mutex> lock(files_mtx_);
  zoneFile = GetFileNoLock(fname);
  return zoneFile;
}

Status ZonedEnv::DeleteFileNoLock(std::string& fname){
  std::shared_ptr<ZoneFile> zoneFile(nullptr);
  Status s;

  fname = FormatPathLexically(fname);
  
  zoneFile = GetFileNoLock(fname);
  //std::cout << "delete fname: " << fname <<" : " << zoneFile <<std::endl;
  if (zoneFile != nullptr) {
    std::string record;

    files_.erase(fname);
    s = zoneFile->RemoveLinkName(fname);
    if (!s.ok()) return s;
    EncodeFileDeletionTo(zoneFile, &record, fname);
    s = PersistRecord(record);
    if (!s.ok()) {
      files_.insert(std::make_pair(fname.c_str(), zoneFile));
      zoneFile->AddLinkName(fname);
    } else {
      if (zoneFile->GetNrLinks() > 0) return s;
       
      zoneFile->SetDeleted();
      zoneFile.reset();
    }
  } else {
    s = target()->DeleteFile(ToAuxPath(fname));
  }

  return s;
}

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
    //
    //files_[dest_path] = source_file;
    files_.insert(std::make_pair(dest_path, source_file));

    s = SyncFileMetadataNoLock(source_file);
    if (!s.ok()) {
      /* Failed to persist the rename, roll back */
      files_.erase(dest_path);
      s = source_file->RenameLink(dest_path, source_path);
      if (!s.ok()) return s;
      //files_[source_path]
      files_.insert(std::make_pair(source_path, source_file));
    }
  } else {
    s = RenameAuxPathNoLock(source_path, dest_path);
  }

  return s;
}

Status ZonedEnv::RenameAuxPathNoLock(const std::string& source_path,
                                    const std::string& dest_path) {
  Status s;
  std::vector<std::string> children;
  std::vector<std::string> renamed_children;

  s = target()->RenameFile(ToAuxPath(source_path), ToAuxPath(dest_path));

  if (!s.ok()) {
    return s;
  }

  GetTaejukChildrenNoLock(source_path, true, &children);

  for (const auto& child : children) {
    s = RenameChildNoLock(source_path, dest_path, child);
    if (!s.ok()) {
      Status failed_rename = s;
      s = RollbackAuxDirRenameNoLock(source_path, dest_path, renamed_children);
      if (!s.ok()) {
        return s;
      }
      return failed_rename;
    }
    renamed_children.push_back(child);
  }

  return s;
}

Status ZonedEnv::RollbackAuxDirRenameNoLock(
    const std::string& source_path, const std::string& dest_path,
    const std::vector<std::string>& renamed_children) {
  Status s;

  for (const auto& rollback_child : renamed_children) {
    s = RenameChildNoLock(dest_path, source_path, rollback_child);
    if (!s.ok()) {
      return Status::Corruption(
          "RollbackAuxDirRenameNoLock: Failed to roll back directory rename");
    }
  }

  s = target()->RenameFile(ToAuxPath(dest_path), ToAuxPath(source_path));
  if (!s.ok()) {
    return Status::Corruption(
        "RollbackAuxDirRenameNoLock: Failed to roll back auxiliary path "
        "renaming");
  }

  return s;
}

Status ZonedEnv::WriteSnapshotLocked(TaejukMetaLog* meta_log){
  Status s;
  std::string snapshot;

  EncodeSnapshotTo(&snapshot);
  s = meta_log->AddRecord(snapshot);
  if (s.ok()) {
    for (auto it = files_.begin(); it != files_.end(); it++) {
      std::shared_ptr<ZoneFile> zoneFile = it->second;
      zoneFile->MetadataSynced();
    }
  }
  return s;
}

Status ZonedEnv::RenameChildNoLock(std::string const& source_dir, std::string const& dest_dir,std::string const& child) {
  std::string source_child = (fs::path(source_dir) / fs::path(child)).string();
  std::string dest_child = (fs::path(dest_dir) / fs::path(child)).string();
  return RenameFileNoLock(source_child, dest_child);
}

Status ZonedEnv::WriteEndRecord(TaejukMetaLog* meta_log){
  std::string endRecord;

  PutFixed32(&endRecord, kEndRecord);
  return meta_log->AddRecord(endRecord);
}
Status ZonedEnv::RollMetaZoneLocked(){
  std::unique_ptr<TaejukMetaLog> new_meta_log, old_meta_log;
  Zone* new_meta_zone = nullptr;
  Status s;

  Status status = zbd_->AllocateMetaZone(&new_meta_zone);
  if (!status.ok()) return status;

  if(!new_meta_zone) {
    return Status::NoSpace("Out of metadata zones");
  }

  new_meta_log.reset(new TaejukMetaLog(zbd_, new_meta_zone));

  old_meta_log.swap(meta_log_);
  meta_log_.swap(new_meta_log);

  if (old_meta_log->GetZone()->GetCapacityLeft())
    WriteEndRecord(old_meta_log.get());
  if (old_meta_log->GetZone()->GetCapacityLeft())
    old_meta_log->GetZone()->Finish();

  std::string super_string;
  superblock_->EncodeTo(&super_string);

  s = meta_log_->AddRecord(super_string);
  if (!s.ok()) {
    return Status::IOError("Failed writing a new superblock");
  }

  s = WriteSnapshotLocked(meta_log_.get());

  if (s.ok()) old_meta_log->GetZone()->Reset();

  return s;
}
Status ZonedEnv::PersistSnapshot(TaejukMetaLog* meta_writer){
  Status s;

  std::lock_guard<std::mutex> file_lock(files_mtx_);
  std::lock_guard<std::mutex> metadata_lock(metadata_sync_mtx_);

  s = WriteSnapshotLocked(meta_writer);
  if(s.IsNoSpace()) {
    s = RollMetaZoneLocked();
  }
  return s;
}
Status ZonedEnv::PersistRecord(std::string record){
  Status s;

  std::lock_guard<std::mutex> lock(metadata_sync_mtx_);
  s = meta_log_->AddRecord(record);
  if(s.IsNoSpace()) s = RollMetaZoneLocked();
  return s;
}

void ZonedEnv::EncodeSnapshotTo(std::string* output){
  std::map<std::string, std::shared_ptr<ZoneFile>>::iterator it;
  std::string files_string;
  PutFixed32(output, kCompleteFilesSnapshot);
  for (it = files_.begin(); it != files_.end(); it++) {
    std::string file_string;
    std::shared_ptr<ZoneFile> zFile = it->second;

    zFile->EncodeSnapshotTo(&file_string);
    PutLengthPrefixedSlice(&files_string, Slice(file_string));
  }
  PutLengthPrefixedSlice(output, Slice(files_string));
}
void ZonedEnv::EncodeFileDeletionTo(std::shared_ptr<ZoneFile> zoneFile, std::string* output, std::string linkf){
  std::string file_string;

  PutFixed64(&file_string, zoneFile->GetID());
  PutLengthPrefixedSlice(&file_string, Slice(linkf));

  PutFixed32(output, kFileDeletion);
  PutLengthPrefixedSlice(output, Slice(file_string));
}

Status ZonedEnv::DecodeSnapshotFrom(Slice* input){
  Slice slice;

  assert(files_.size() == 0);

  while (GetLengthPrefixedSlice(input, &slice)) {
    std::shared_ptr<ZoneFile> zoneFile(
        new ZoneFile(zbd_, 0, &metadata_writer_));
    Status s = zoneFile->DecodeFrom(&slice);
    if (!s.ok()) return s;

    if (zoneFile->GetID() >= next_file_id_)
      next_file_id_ = zoneFile->GetID() + 1;

    for (const auto& name : zoneFile->GetLinkFiles())
      files_.insert(std::make_pair(name, zoneFile));
  }

  return Status::OK();
}

Status ZonedEnv::DecodeFileUpdateFrom(Slice* slice, bool replace){
  std::shared_ptr<ZoneFile> update(new ZoneFile(zbd_, 0, &metadata_writer_));
  uint64_t id;
  Status s;

  s = update->DecodeFrom(slice);
  if (!s.ok()) return s;

  id = update->GetID();
  if (id >= next_file_id_) next_file_id_ = id + 1;

  for (auto it = files_.begin(); it != files_.end(); it++) {
    std::shared_ptr<ZoneFile> zFile = it->second;
    if (id == zFile->GetID()) {
      for (const auto& name : zFile->GetLinkFiles()) {
        if (files_.find(name) != files_.end())
          files_.erase(name);
        else
          return Status::Corruption("DecodeFileUpdateFrom: missing link file");
      }

      s = zFile->MergeUpdate(update, replace);
      update.reset();

      if (!s.ok()) return s;

      for (const auto& name : zFile->GetLinkFiles())
        files_.insert(std::make_pair(name, zFile));

      return Status::OK();
    }
  }

  /* The update is a new file */
  assert(GetFile(update->GetFilename()) == nullptr);
  files_.insert(std::make_pair(update->GetFilename(), update));

  return Status::OK();
}
Status ZonedEnv::DecodeFileDeletionFrom(Slice* input){
  uint64_t fileID;
  std::string fileName;
  Slice slice;
  Status s;

  if (!GetFixed64(input, &fileID))
    return Status::Corruption("Zone file deletion: file id missing");

  if (!GetLengthPrefixedSlice(input, &slice))
    return Status::Corruption("Zone file deletion: file name missing");

  fileName = slice.ToString();
  if (files_.find(fileName) == files_.end())
    return Status::Corruption("Zone file deletion: no such file");

  std::shared_ptr<ZoneFile> zoneFile = files_[fileName];
  if (zoneFile->GetID() != fileID)
    return Status::Corruption("Zone file deletion: file ID missmatch");

  files_.erase(fileName);
  s = zoneFile->RemoveLinkName(fileName);
  if (!s.ok())
    return Status::Corruption("Zone file deletion: file links missmatch");

  return Status::OK();
}
Status ZonedEnv::RecoverFrom(TaejukMetaLog* log){
  bool at_least_one_snapshot = false;
  std::string scratch;
  uint32_t tag = 0;
  Slice record;
  Slice data;
  Status s;
  bool done = false;

  while (!done) {
    Status rs = log->ReadRecord(&record, &scratch);
    if (!rs.ok()) {
      return Status::Corruption("ZenFS", "Metadata corruption");
    }

    if (!GetFixed32(&record, &tag)) break;

    if (tag == kEndRecord) break;

    if (!GetLengthPrefixedSlice(&record, &data)) {
      return Status::Corruption("ZenFS", "No recovery record data");
    }

    switch (tag) {
      case kCompleteFilesSnapshot:
        ClearFiles();
        s = DecodeSnapshotFrom(&data);
        if (!s.ok()) {
          return s;
        }
        at_least_one_snapshot = true;
        break;

      case kFileUpdate:
        s = DecodeFileUpdateFrom(&data);
        if (!s.ok()) {
          return s;
        }
        break;

      case kFileReplace:
        s = DecodeFileUpdateFrom(&data, true);
        if (!s.ok()) {
          return s;
        }
        break;

      case kFileDeletion:
        s = DecodeFileDeletionFrom(&data);
        if (!s.ok()) {
          return s;
        }
        break;

      default:
        return Status::Corruption("ZenFS", "Unexpected tag");
    }
  }

  if (at_least_one_snapshot)
    return Status::OK();
  else
    return Status::NotFound("ZenFS", "No snapshot found");
}

std::string ZonedEnv::FormatPathLexically(fs::path filepath){
  fs::path ret = fs::path("/") / filepath.lexically_normal();
  return ret.string();
}

Status NewZonedEnv(Env** env, const std::string& bdevname) {

  ZonedBlockDevice* zbd = new ZonedBlockDevice(bdevname);
  
  Status s = zbd->Open(false, true);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    delete zbd;
    return s;
  }

  ZonedEnv* zenv = new ZonedEnv(zbd);
  s = zenv->Mount(false);
  if (!s.ok()) {
    delete zenv; 
    return s;
  }

  *env = zenv;
  return Status::OK();
}

}

