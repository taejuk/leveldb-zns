#pragma once
#include <filesystem>
namespace fs = std::filesystem;
#include <memory>
#include <thread>
#include <map>
#include "leveldb/zenfs/io_taejuk.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "snapshot.h"
#include "leveldb/zenfs/zbd_taejuk.h"
#include <atomic>
// envwrapper를 상속받아서 구현하자.

namespace leveldb {

class ZoneSnapshot;
class ZoneFileSnapshot;
class TaejukSnapshot;
class TaejukSnapshotOptions;

class Superblock {
  uint32_t magic_ = 0;
  char uuid_[37] = {0};
  uint32_t sequence_ = 0;
  uint32_t flags_ = 0;
  uint32_t block_size_ = 0; /* in bytes */
  uint32_t zone_size_ = 0;  /* in blocks */
  uint32_t nr_zones_ = 0;
  char aux_fs_path_[256] = {0};
  uint32_t finish_treshold_ = 0;
  char reserved_[191] = {0};

 public:
  const uint32_t MAGIC = 0x7461656a; // taej
  const uint32_t ENCODED_SIZE = 512;
  const uint32_t DEFAULT_FLAGS = 0;
  const uint32_t FLAGS_ENABLE_GC = 1 << 0;

  Superblock() {}

  Superblock(ZonedBlockDevice* zbd, std::string aux_fs_path = "",
             uint32_t finish_threshold = 0, bool enable_gc = false) {
    std::string uuid = "test";
    //std::string uuid = Env::Default()->GenerateUniqueId();
    int uuid_len = std::min(uuid.length(), sizeof(uuid_) - 1);
    memcpy((void*)uuid_, uuid.c_str(), uuid_len);
    magic_ = MAGIC;
    flags_ = DEFAULT_FLAGS;
    if (enable_gc) flags_ |= FLAGS_ENABLE_GC;

    finish_treshold_ = finish_threshold;

    block_size_ = zbd->GetBlockSize();
    zone_size_ = zbd->GetZoneSize() / block_size_;
    nr_zones_ = zbd->GetNrZones();

    strncpy(aux_fs_path_, aux_fs_path.c_str(), sizeof(aux_fs_path_) - 1);
  }

  Status DecodeFrom(Slice* input);
  void EncodeTo(std::string* output);
  Status CompatibleWith(ZonedBlockDevice* zbd);
  void GetReport(std::string* reportString);

  uint32_t GetSeq() { return sequence_; }
  std::string GetAuxFsPath() { return std::string(aux_fs_path_); }
  uint32_t GetFinishTreshold() { return finish_treshold_; }
  std::string GetUUID() { return std::string(uuid_); }
  bool IsGCEnabled() { return flags_ & FLAGS_ENABLE_GC; };
};

class TaejukMetaLog {
  uint64_t read_pos_;
  Zone* zone_;
  ZonedBlockDevice* zbd_;
  size_t bs_;

  const size_t zMetaHeaderSize = sizeof(uint32_t) * 2;

 public:
  TaejukMetaLog(ZonedBlockDevice* zbd, Zone* zone) {
    assert(zone->IsBusy());
    zbd_ = zbd;
    zone_ = zone;
    bs_ = zbd_->GetBlockSize();
    read_pos_ = zone->start_;
  }

  virtual ~TaejukMetaLog() {
    bool ok = zone_->Release();
    assert(ok);
    (void)ok;
  }

  Status AddRecord(const Slice& slice);
  Status ReadRecord(Slice* record, std::string* scratch);

  Zone* GetZone() { return zone_; };

 private:
  Status Read(Slice* slice);
};

class LEVELDB_EXPORT ZonedEnv : public EnvWrapper {
 public:
  explicit ZonedEnv(ZonedBlockDevice* zbd, Env* target = Env::Default());
  ~ZonedEnv() override;

  Status Mount(bool readonly);
  Status MkFS(std::string aux_fs_path, uint32_t finish_threshold, bool enable_gc);
  std::map<std::string, WriteLifeTimeHint> GetWriteLifeTimeHints();
  
  Status NewSequentialFile(const std::string& fname, SequentialFile** result) override;
  Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result) override;
  Status NewWritableFile(const std::string& fname, WritableFile** result,WriteLifeTimeHint hint = WLTH_NOT_SET) override;
  Status NewAppendableFile(const std::string& fname, WritableFile** result,WriteLifeTimeHint hint = WLTH_NOT_SET) override;

  Status OpenWritableFile(const std::string& fname, WritableFile** result, WriteLifeTimeHint hint, bool reopen);

  bool FileExists(const std::string& fname) override;
  Status GetChildren(const std::string& dir, std::vector<std::string>* result) override;
  Status GetChildrenNoLock(const std::string& dir, std::vector<std::string>* result);
  void GetTaejukChildrenNoLock(const std::string& dir, bool include_grandchildren, std::vector<std::string>* result);
  Status DeleteFile(const std::string& fname) override;
  Status GetFileSize(const std::string& fname, uint64_t* file_size) override;
  Status RenameFile(const std::string& src, const std::string& target) override;
  Status RenameChildNoLock(std::string const& source_dir, std::string const& dest_dir,std::string const& child);

  Status CreateDir(const std::string& d) {
    return target()->CreateDir(ToAuxPath(d));
  }
  Status CreateDirIfMissing(const std::string& d) {
    return Status::OK();
    //return target()->CreateDirIfMissing(ToAuxPath(d));
  }
  Status DeleteDir(const std::string& dirname) override;
  Status LockFile(const std::string& fname, FileLock** lock) override;
  Status UnlockFile(FileLock* lock) override;
  Status GetTestDirectory(std::string* path) override;
  Status NewLogger(const std::string& fname, Logger** result) override;


  void GetTaejukSnapshot(TaejukSnapshot& snapshot, const TaejukSnapshotOptions& options);
  Status MigrateExtents(const std::vector<ZoneExtentSnapshot*>& extents);
  Status MigrateFileExtents(const std::string& fname, const std::vector<ZoneExtentSnapshot*>& migrate_exts);

 private:
  enum ZenFSTag : uint32_t {
    kCompleteFilesSnapshot = 1,
    kFileUpdate = 2,
    kFileDeletion = 3,
    kEndRecord = 4,
    kFileReplace = 5,
  };

  ZonedBlockDevice* zbd_;
  
  std::map<std::string, std::shared_ptr<ZoneFile>> files_;
  std::mutex files_mtx_;
  
  std::atomic<uint64_t> next_file_id_;

  Zone* cur_meta_zone_ = nullptr;
  std::unique_ptr<TaejukMetaLog> meta_log_;
  std::mutex metadata_sync_mtx_;
  std::unique_ptr<Superblock> superblock_;

  std::unique_ptr<std::thread> gc_worker_ = nullptr;
  bool run_gc_worker_ = false;
  const uint64_t GC_START_LEVEL = 20; 
  const uint64_t GC_SLOPE = 3;
  
  void GCWorker();

  std::string ToAuxPath(const std::string& path) {
    return superblock_->GetAuxFsPath() + path;
  }
  
  void ClearFiles();
  std::string FormatPathLexically(fs::path filepath);
  
  std::shared_ptr<ZoneFile> GetFileNoLock(std::string fname);
  std::shared_ptr<ZoneFile> GetFile(std::string fname);
  Status DeleteFileNoLock(std::string& fname);
  Status SyncFileMetadataNoLock(ZoneFile* zoneFile, bool replace = false);
  Status SyncFileMetadataNoLock(std::shared_ptr<ZoneFile> zoneFile,
                                  bool replace = false) {
    return SyncFileMetadataNoLock(zoneFile.get(), replace);
  }
  Status SyncFileMetadata(ZoneFile* zoneFile, bool replace = false);
  Status SyncFileMetadata(std::shared_ptr<ZoneFile> zoneFile,
                            bool replace = false) {
    return SyncFileMetadata(zoneFile.get(), replace);
  }
  Status SyncFileExtents(ZoneFile* zoneFile, std::vector<ZoneExtent*> new_extents);
  Status RenameFileNoLock(const std::string& src, const std::string& target);
  Status RenameAuxPathNoLock(const std::string& source_path, const std::string& dest_path);
  Status RollbackAuxDirRenameNoLock(
    const std::string& source_path, const std::string& dest_path,
    const std::vector<std::string>& renamed_children);

  Status WriteSnapshotLocked(TaejukMetaLog* meta_log);
  Status WriteEndRecord(TaejukMetaLog* meta_log);
  Status RollMetaZoneLocked();
  Status PersistSnapshot(TaejukMetaLog* meta_writer);
  Status PersistRecord(std::string record);
  Status Repair();

  void EncodeSnapshotTo(std::string* output);
  void EncodeFileDeletionTo(std::shared_ptr<ZoneFile> zoneFile, std::string* output, std::string linkf);
  
  Status DecodeSnapshotFrom(Slice* input);
  Status DecodeFileUpdateFrom(Slice* slice, bool replace = false);
  Status DecodeFileDeletionFrom(Slice* slice);
  Status RecoverFrom(TaejukMetaLog* log);

  struct TaejukMetadataWriter : public MetadataWriter {
    ZonedEnv* env;
    Status Persist(ZoneFile* zoneFile) override {
      return env->SyncFileMetadata(zoneFile);
    }
  };
  TaejukMetadataWriter metadata_writer_;
};

Status NewZonedEnv(Env** env, const std::string& bdevname);

}