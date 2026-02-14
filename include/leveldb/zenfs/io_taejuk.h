#pragma once

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/status.h"

#include "leveldb/zenfs/zbd_taejuk.h" 

namespace leveldb {

// 전방 선언
class ZoneFile;

// 1. ZoneExtent
class ZoneExtent {
public:
  uint64_t start_;
  uint64_t length_;
  Zone* zone_;

  explicit ZoneExtent(uint64_t start, uint64_t length, Zone* zone);
  Status DecodeFrom(Slice* input);
  void EncodeTo(std::string* output);
  void EncodeJson(std::ostream& json_stream);
};

// 2. MetadataWriter (인터페이스)
class MetadataWriter {
public:
  virtual ~MetadataWriter(); // [수정] 오타 수정 (~MetadataWrite -> ~MetadataWriter)
  virtual Status Persist(ZoneFile* zoneFile) = 0;
};

// 3. ZoneFile (파일 메타데이터 관리)
class ZoneFile {
private:
  const uint64_t NO_EXTENT = 0xffffffffffffffff;
  ZonedBlockDevice* zbd_;

  std::vector<ZoneExtent*> extents_;
  std::vector<std::string> linkfiles_;

  Zone* active_zone_;
  uint64_t extent_start_ = NO_EXTENT;
  uint64_t extent_filepos_ = 0;

  // env.h에 WriteLifeTimeHint를 추가했다고 가정
  WriteLifeTimeHint lifetime_; 
  uint64_t file_size_;
  uint64_t file_id_;

  uint32_t nr_synced_extents_ = 0;
  bool open_for_wr_ = false;
  std::mutex open_for_wr_mtx_;

  time_t m_time_;
  bool is_deleted_ = false;

  MetadataWriter* metadata_writer_ = NULL;

  std::mutex writer_mtx_;
  std::atomic<int> readers_{0};
  IOType io_type_;

public:
  explicit ZoneFile(ZonedBlockDevice* zbd, uint64_t file_id,
                    MetadataWriter* metadata_writer);

  virtual ~ZoneFile();

  void AcquireWRLock();
  bool TryAcquireWRLock();
  void ReleaseWRLock();
  void SetIOType(IOType io_type);
  Status CloseWR();
  bool IsOpenForWR();

  Status PersistMetadata();

  Status Append(void* buffer, int data_size);
  Status BufferedAppend(char* data, uint32_t size);
  

  Status SetWriteLifeTimeHint(WriteLifeTimeHint lifetime);
  std::string GetFilename();
  time_t GetFileModificationTime();
  void SetFileModificationTime(time_t mt);
  uint64_t GetFileSize();
  void SetFileSize(uint64_t sz);
  void ClearExtents();

  uint32_t GetBlockSize() { return zbd_->GetBlockSize(); }
  ZonedBlockDevice* GetZbd() { return zbd_; }
  std::vector<ZoneExtent*> GetExtents() { return extents_; }
  WriteLifeTimeHint GetWriteLifeTimeHint() { return lifetime_; }

  Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                          char* scratch, bool direct);
  ZoneExtent* GetExtent(uint64_t file_offset, uint64_t* dev_offset);
  void PushExtent();
  
  Status AllocateNewZone(); 

  void EncodeTo(std::string* output, uint32_t extent_start);
  void EncodeUpdateTo(std::string* output) {
    EncodeTo(output, nr_synced_extents_);
  };
  void EncodeSnapshotTo(std::string* output) { EncodeTo(output, 0); };
  void EncodeJson(std::ostream& json_stream);
  void MetadataSynced() { nr_synced_extents_ = extents_.size(); };
  void MetadataUnsynced() { nr_synced_extents_ = 0; };

  Status MigrateData(uint64_t offset, uint32_t length, Zone* target_zone); // [수정] IOStatus -> Status

  Status DecodeFrom(Slice* input);
  Status MergeUpdate(std::shared_ptr<ZoneFile> update, bool replace);

  uint64_t GetID() { return file_id_; }
  
  uint64_t HasActiveExtent() { return extent_start_ != NO_EXTENT; };
  uint64_t GetExtentStart() { return extent_start_; };

  Status Recover();

  void ReplaceExtentList(std::vector<ZoneExtent*> new_list);
  void AddLinkName(const std::string& linkfile);
  Status RemoveLinkName(const std::string& linkfile);
  Status RenameLink(const std::string& src, const std::string& dest);
  uint32_t GetNrLinks() { return linkfiles_.size(); }
  const std::vector<std::string>& GetLinkFiles() const { return linkfiles_; }

  Status InvalidateCache(uint64_t pos, uint64_t size);

private:
  void ReleaseActiveZone();
  void SetActiveZone(Zone* zone);
  Status CloseActiveZone();

public:
  bool IsDeleted() const { return is_deleted_; };
  void SetDeleted() { is_deleted_ = true; };

public:
  class ReadLock {
   public:
    ReadLock(ZoneFile* zfile) : zfile_(zfile) {
      zfile_->writer_mtx_.lock();
      zfile_->readers_++;
      zfile_->writer_mtx_.unlock();
    }
    ~ReadLock() { zfile_->readers_--; }
   private:
    ZoneFile* zfile_;
  };
  class WriteLock {
   public:
    WriteLock(ZoneFile* zfile) : zfile_(zfile) {
      zfile_->writer_mtx_.lock();
      while (zfile_->readers_ > 0) {
      }
    }
    ~WriteLock() { zfile_->writer_mtx_.unlock(); }
   private:
    ZoneFile* zfile_;
  };
};

class ZonedWritableFile : public WritableFile {
 public:
  explicit ZonedWritableFile(ZonedBlockDevice* zbd, bool buffered,
                             std::shared_ptr<ZoneFile> zoneFile);
  
  virtual ~ZonedWritableFile();

  virtual Status Append(const Slice& data) override;
  virtual Status Close() override;
  virtual Status Flush() override;
  virtual Status Sync() override;

  std::string GetName() const { return zoneFile_->GetFilename(); }

  void SetWriteLifeTimeHint(WriteLifeTimeHint hint); 

 private:
  Status BufferedWrite(const Slice& data);
  Status FlushBuffer();
  Status DataSync();
  Status CloseInternal();

  bool buffered;
  char* buffer;
  size_t buffer_sz;
  uint32_t block_sz;
  uint32_t buffer_pos;
  uint64_t wp;
  int write_temp;
  bool open;

  std::shared_ptr<ZoneFile> zoneFile_;
  MetadataWriter* metadata_writer_;

  std::mutex buffer_mtx_;
};

class ZonedSequentialFile : public SequentialFile {
private:
  std::shared_ptr<ZoneFile> zoneFile_;
  uint64_t rp;
  bool direct_;

public:
  explicit ZonedSequentialFile(std::shared_ptr<ZoneFile> zoneFile)
      : zoneFile_(zoneFile),
        rp(0),
        direct_(false) {}

  Status Read(size_t n, Slice* result, char* scratch) override;
  Status Skip(uint64_t n) override;
  
  bool use_direct_io() const { return direct_; } 

  size_t GetRequiredBufferAlignment() const {
    return zoneFile_->GetBlockSize();
  }

  Status InvalidateCache(size_t offset, size_t length) {
    return zoneFile_->InvalidateCache(offset, length);
  }
};

class ZonedRandomAccessFile : public RandomAccessFile {
private:
  std::shared_ptr<ZoneFile> zoneFile_;
  bool direct_;

public:
  explicit ZonedRandomAccessFile(std::shared_ptr<ZoneFile> zoneFile) 
      : zoneFile_(zoneFile), direct_(false) {} 

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override;

  bool use_direct_io() const { return direct_; }

  size_t GetRequiredBufferAlignment() const {
    return zoneFile_->GetBlockSize();
  }

  Status InvalidateCache(size_t offset, size_t length) {
    return zoneFile_->InvalidateCache(offset, length);
  }
};

}