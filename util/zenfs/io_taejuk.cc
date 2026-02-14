#include "leveldb/zenfs/io_taejuk.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "leveldb/env.h"
#include "util/coding.h"

namespace leveldb {

ZoneExtent::ZoneExtent(uint64_t start, uint64_t length, Zone* zone)
    : start_(start), length_(length), zone_(zone) {}

Status ZoneExtent::DecodeFrom(Slice* input) {
  if (input->size() != (sizeof(start_) + sizeof(length_)))
    return Status::Corruption("ZoneExtent", "Error: length missmatch");

  GetFixed64(input, &start_);
  GetFixed64(input, &length_);
  return Status::OK();
}

void ZoneExtent::EncodeTo(std::string* output) {
  PutFixed64(output, start_);
  PutFixed64(output, length_);
}

void ZoneExtent::EncodeJson(std::ostream& json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"length\":" << length_;
  json_stream << "}";
}

enum ZoneFileTag : uint32_t {
  kFileID = 1,
  kFileNameDeprecated = 2,
  kFileSize = 3,
  kWriteLifeTimeHint = 4,
  kExtent = 5,
  kModificationTime = 6,
  kActiveExtentStart = 7,
  kIsSparse = 8,
  kLinkedFilename = 9,
};

void ZoneFile::EncodeTo(std::string* output, uint32_t extent_start) {
  PutFixed32(output, kFileID);
  PutFixed64(output, file_id_);

  PutFixed32(output, kFileSize);
  PutFixed64(output, file_size_);

  PutFixed32(output, kWriteLifeTimeHint);
  PutFixed32(output, (uint32_t)lifetime_);

  for (uint32_t i = extent_start; i < extents_.size(); i++) {
    std::string extent_str;

    PutFixed32(output, kExtent);
    extents_[i]->EncodeTo(&extent_str);
    PutLengthPrefixedSlice(output, Slice(extent_str));
  }

  PutFixed32(output, kModificationTime);
  PutFixed64(output, (uint64_t)m_time_);

  PutFixed32(output, kActiveExtentStart);
  PutFixed64(output, extent_start_);

  if (is_sparse_) {
    PutFixed32(output, kIsSparse);
  }

  for (uint32_t i = 0; i < linkfiles_.size(); i++) {
    PutFixed32(output, kLinkedFilename);
    PutLengthPrefixedSlice(output, Slice(linkfiles_[i]));
  }
}

void ZoneFile::EncodeJson(std::ostream& json_stream) {
  json_stream << "{";
  json_stream << "\"id\":" << file_id_ << ",";
  json_stream << "\"size\":" << file_size_ << ",";
  json_stream << "\"hint\":" << lifetime_ << ",";
  json_stream << "\"filename\":[";

  bool first_element = true;
  for (const auto& name : GetLinkFiles()) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    json_stream << "\"" << name << "\"";
  }
  json_stream << "],";

  json_stream << "\"extents\":[";

  first_element = true;
  for (ZoneExtent* extent : extents_) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    extent->EncodeJson(json_stream);
  }
  json_stream << "]}";
}



Status ZoneFile::DecodeFrom(Slice* input) {
  // uint32_t tag = 0;
  // GetFiexed32(input, &tag);

  // if (tag != kFileID || !GetFiexed64(input, &file_id_))
  //   return Status::Corruption("ZoneFile", "File ID missing");


  return Status::OK();
}

Status ZoneFile::MergeUpdate(std::shared_ptr<ZoneFile> update, bool replace) {
  // if (file_id_ != update->GetID()) return Status::Corruption("ZoneFile update", "ID missmatch");

  // SetFileSize(update->GetFileSize());
  return Status::OK();
}

ZoneFile::ZoneFile(ZonedBlockDevice* zbd, uint64_t file_id, MetadataWriter* metadata_writer)
    : zbd_(zbd),
      active_zone_(NULL),
      extent_start_(NO_EXTENT),
      extent_filepos_(0),
      lifetime_(WLTH_NOT_SET),
      io_type_(kUnknown),
      file_size_(0),
      file_id_(file_id),
      nr_synced_extents_(0),
      m_time_(0),
      metadata_writer_(metadata_writer) {}

std::string ZoneFile::GetFilename() { return linkfiles_[0]; }
time_t ZoneFile::GetFileModificationTime() { return m_time_; }

uint64_t ZoneFile::GetFileSize() { return file_size_; }
void ZoneFile::SetFileSize(uint64_t sz) { file_size_ = sz; }
void ZoneFile::SetFileModificationTime(time_t mt) { m_time_ = mt; }
void ZoneFile::SetIOType(IOType io_type) { io_type_ = io_type; }

ZoneFile::~ZoneFile() { ClearExtents(); }

void ZoneFile::ClearExtents() {
  for (auto e = std::begin(extents_); e != std::end(extents_); ++e) {
    Zone* zone = (*e)->zone_;

    assert(zone && zone->used_capacity_ >= (*e)->length_);
    zone->used_capacity_ -= (*e)->length_;
    delete *e;
  }
  extents_.clear();
}

Status ZoneFile::CloseActiveZone() {
  Status s = Status::OK();
  if(active_zone_) {
    bool full = active_zone_->IsFull();
    s = active_zone_->Close();
    if(!s.ok()) return s;
    // token들은 active와 open수를 관리하기 위한 것이다.
    zbd_->PutOpenIOZoneToken();
    if(full) zbd_->PutActiveIOZoneToken();
  }

  return s;
}

void ZoneFile::AcquireWRLock() {
  open_for_wr_mtx_.lock();
  open_for_wr_ = true;
}

bool ZoneFile::TryAcquireWRLock() {
  if (!open_for_wr_mtx_.try_lock()) return false;
  open_for_wr_ = true;
  return true;
}

void ZoneFile::ReleaseWRLock() {
  assert(open_for_wr_);
  open_for_wr_ = false;
  open_for_wr_mtx_.unlock();
}

bool ZoneFile::IsOpenForWR() { return open_for_wr_; }

Status ZoneFile::CloseWR() {
  Status s;

  extent_start_ = NO_EXTENT;
  s = PersistMetadata();
  if (!s.ok()) return s;
  ReleaseWRLock();
  return CloseActiveZone();
}

Status ZoneFile::PersistMetadata() {
  assert(metadata_writer_ != NULL);
  return metadata_writer_->Persist(this);
}

ZoneExtent* ZoneFile::GetExtent(uint64_t file_offset, uint64_t* dev_offset) {
  for(unsigned int i = 0; i < extents_.size(); i++) {
    if (file_offset < extents_[i]->length_) {
      *dev_offset = extents_[i]->start_ + file_offset;
      return extents_[i];
    } else {
      file_offset -= extents_[i]->length_;
    }
  }
  return NULL;
}

Status ZoneFile::InvalidateCache(uint64_t pos, uint64_t size) {
  ReadLock lck(this);
  uint64_t offset = pos;
  uint64_t left = size;
  Status s = Status::OK();

  if (left == 0) {
    left = GetFileSize();
  }

  while (left) {
    uint64_t dev_offset;
    ZoneExtent* extent = GetExtent(offset, &dev_offset);

    if(!extent) {
      s = Status::IOError("Extent not found while invalidating cache");
      break;
    }

    uint64_t extent_end = extent->start_ + extent->length_;
    uint64_t invalidate_size = std::min(left, extent_end - dev_offset);

    s = zbd_->InvalidateCache(dev_offset, invalidate_size);
    if(!s.ok()) break;

    left -= invalidate_size;
    offset += invalidate_size;
  }
  return s;
}

Status ZoneFile::PositionedRead(uint64_t offset, size_t n, Slice* result, char* scratch, bool direct) {
  ReadLock lck(this);

  char* ptr;
  uint64_t r_off;
  size_t r_sz;
  ssize_t r = 0;
  size_t read = 0;
  ZoneExtent* extent;
  uint64_t extent_end;
  Status s;

  if (offset >= file_size_) {
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  r_off = 0;
  extent = GetExtent(offset, &r_off);
  if(!extent) {
    *result = Slice(scratch, 0);
    return s;
  }
  extent_end = extent->start_ + extent->length_;

  if ((offset + n) > file_size_) r_sz = file_size_ - offset;
  else r_sz = n;

  ptr = scratch;

  while (read != r_sz) {
    // 읽어야 하는 사이즈
    size_t pread_sz = r_sz - read;
    // extent를 넘어가면 안되니깐
    if ((pread_sz + r_off) > extent_end) pread_sz = extent_end - r_off;

    bool aligned = (pread_sz % zbd_->GetBlockSize() == 0);
    size_t bytes_to_align = 0;
    if (direct && !aligned) {
      bytes_to_align = zbd_->GetBlockSize() - (pread_sz % zbd_->GetBlockSize());
      pread_sz += bytes_to_align;
      aligned = true;
    }
    // 읽은 사이즈
    r = zbd_->Read(ptr, r_off, pread_sz, direct && aligned);
    if (r <= 0) break;

    if((size_t)r <= pread_sz - bytes_to_align) pread_sz = (size_t)r;
    // align했는데 더 읽은 것이다
    else pread_sz -= bytes_to_align;

    ptr += pread_sz;
    read += pread_sz;
    r_off += pread_sz;
    // 아직 덜 읽었는데, 해당 extent에서는 끝까지 읽었을 때
    if (read != r_sz && r_off == extent_end) {
      extent = GetExtent(offset + read, &r_off);
      if (!extent) {
        break;
      }
      r_off = extent->start_;
      extent_end = extent->start_ + extent->length_;
    }
  }

  if (r < 0) {
    s = Status::Error("pread error\n");
    read = 0;
  }

  *result = Slice((char*)scratch, read);
  return s;
}
// 현재까지 쓴 데이터를 하나의 extent로 보고 저장한다.
// 가변 길이 가능
void ZoneFile::PushExtent() {
  uint64_t length;

  assert(file_size_ >= extent_filepos_);

  if (!active_zone_) return;

  length = file_size_ - extent_filepos_;
  if(length == 0) return;

  assert(length <= (active_zone_->wp_ - extent_start_));
  extents_.push_back(new ZoneExtent(extent_start_, length, active_zone_));

  active_zone_->used_capacity_ += length;
  extent_start_ = active_zone_->wp_;
  extent_filepos_ = file_size_;
}

Status ZoneFile::AllocateNewZone() {
  Zone* zone;
  Status s = zbd_->AllocateIOZone(lifetime_, io_type_, &zone);

  if (!s.ok()) return s;
  if (!zone) {
    return Status::NoSpace("Zone allocation failure\n");
  }
  SetActiveZone(zone);
  extent_start_ = active_zone_->wp_;
  extent_filepos_ = file_size_;

  return PersistMetadata();
}

Status ZoneFile::BufferedAppend(char* buffer, uint32_t data_size) {
  uint32_t left = data_size;
  uint32_t wr_size;
  uint32_t block_sz = GetBlockSize();

  Status s;
  if (active_zone_ == NULL) {
    s = AllocateNewZone();
    if (!s.ok()) return s;
  }

  while(left) {
    wr_size = left;
    if(wr_size > active_zone_->capacity_) wr_size = active_zone_->capacity_;

    uint32_t align = wr_size % block_sz;
    uint32_t pad_sz = 0;
    if (align) pad_sz = block_sz - align;

    if(pad_sz) memset(buffer + wr_size, 0x0, pad_sz);
    uint64_t extent_length = wr_size;

    s = active_zone_->Append(buffer, wr_size + pad_sz);
    if (!s.ok()) return s;

    extents_.push_back(
      new ZoneExtent(extent_start_, extent_length, active_zone_)
    );

    extent_start_ = active_zone_->wp_;
    active_zone_->used_capacity_ += extent_length;
    file_size_ += extent_length;
    left -= extent_length;

    if (active_zone_->capacity_ == 0) {
      s = CloseActiveZone();
      if (!s.ok()) {
        return s;
      }
      if (left) {
        memmove((void*)(buffer), (void*)(buffer + wr_size), left);
      }
      s = AllocateNewZone();
      if (!s.ok()) return s;
    }

  }

  return Status::OK();
}

Status ZoneFile::Append(void* data, int data_size) {
  uint32_t left = data_size;
  uint32_t wr_size, offset = 0;
  Status s = Status::OK();

  if (!active_zone_) {
    s = AllocateNewZone();
    if(!s.ok()) return s;
  }

  while (left) {
    // 다 썼으니깐
    // 이때까지 쓴 데이터 저장하고
    // 새로운 존을 만든다.
    if (active_zone_->capacity_ == 0) {
      PushExtent();

      s = CloseActiveZone();
      if (!s.ok()) {
        return s;
      }

      s = AllocateNewZone();
      if(!s.ok()) return s;
    }

    wr_size = left;
    if (wr_size > active_zone_->capacity_) wr_size = active_zone_->capacity_;

    s = active_zone_->Append((char*)data+offset, wr_size);
    if (!s.ok()) return s;

    file_size_ += wr_size;
    left -= wr_size;
    offset += wr_size;
  }
  return Status::OK();
}

Status ZoneFile::Recover() {
  if(!HasActiveExtent()) return Status::OK();

  Zone* zone = zbd_->GetIOZone(extent_start_);

  if(zone == nullptr){
    return Status::IOError("Could not find zone for extent start while recovering");
  }

  if (zone->wp_ < extent_start_) {
    return Status::IOError("Zone wp is smaller than active extent start");
  }
  // 현재 writer point와 extent_start_ 사이를 복구한다.
  uint64_t to_recover = zone->wp_ - extent_start_;

  if (to_recover == 0) {
    extent_start_ = NO_EXTENT;
    return Status::OK();
  }

  zone->used_capacity_ += to_recover;
  extents_.push_back(new ZoneExtent(extent_start_, to_recover, zone));

  extent_start_ = NO_EXTENT;

  file_size_ = 0;
  for (uint32_t i = 0; i < extents_.size(); i++) {
    file_size_ += extents_[i]->length_;
  }

  return Status::OK();
}

void ZoneFile::ReplaceExtentList(std::vector<ZoneExtent*> new_list) {
  assert(IsOpenForWR() && new_list.size() > 0);
  assert(new_list.size() == extents_.size());

  WriteLock lck(this);
  extents_ = new_list;
}

void ZoneFile::AddLinkName(const std::string& linkf) {
  linkfiles_.push_back(linkf);
}

Status ZoneFile::RenameLink(const std::string& src, const std::string& dest) {
  auto itr = std::find(linkfiles_.begin(), linkfiles_.end(), src);
  if (itr != linkfiles_.end()) {
    linkfiles_.erase(itr);
    linkfiles_.push_back(dest);
  } else {
    return Status::IOError("RenameLink: Failed to find the linked file");
  }
  return Status::OK();
}

Status ZoneFile::RemoveLinkName(const std::string& linkf) {
  assert(GetNrLinks());
  auto itr = std::find(linkfiles_.begin(), linkfiles_.end(), linkf);
  if (itr != linkfiles_.end()) {
    linkfiles_.erase(itr);
  } else {
    return Status::IOError("RemoveLinkInfo: Failed to find the link file");
  }
  return Status::OK();
}

Status ZoneFile::SetWriteLifeTimeHint(WriteLifeTimeHint lifetime) {
  lifetime_ = lifetime;
  return Status::OK();
}

void ZoneFile::ReleaseActiveZone() {
  assert(active_zone_ != nullptr);
  bool ok = active_zone_->Release();
  // release mode로 하면 assert사라진다. 그러면 경고 띄우는데
  // void를 통해서 경고를 안띄우도록 한다.
  assert(ok);
  (void)ok;
  active_zone_ = nullptr;
}

void ZoneFile::SetActiveZone(Zone* zone) {
  assert(active_zone_ == nullptr);
  assert(zone->IsBusy());
  active_zone_ = zone;
}

ZonedWritableFile::ZonedWritableFile(ZonedBlockDevice* zbd, bool buffered,
                             std::shared_ptr<ZoneFile> zoneFile) {
  assert(zoneFile->IsOpenForWR());
  wp = zoneFile->GetFileSize();

  buffered = _buffered;
  block_sz = zbd->GetBlockSize();
  zoneFile_ = zoneFile;
  buffer_pos = 0;
  buffer = nullptr;
  if(buffered) {
    buffer_sz = 1024*1024;
    int ret = posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), buffer_sz);
    
    if(ret) buffer = nullptr;
    assert(buffer != nullptr);
  }

  open = true;
}

ZonedWritableFile::~ZonedWritableFile() {
  Status s = CloseInternal();
  if(buffered) {
    free(buffer);
  }
  if (!s.ok()) zoneFile_->GetZbd()->SetZoneDeferredStatus(s);
}

MetadataWriter::~MetadataWriter() {}


Status ZonedWritableFile::Append(const Slice& data){
  Status s;

  if (buffered) {
    buffer_mtx_.lock();
    s = BufferedWrite(data);
    buffer_mtx_.unlock();
  } else {
    s = zoneFile_->Append((void*)data.data(), data.size());
    if (s.ok()) wp+= data.size();
  }

  return s;
}
Status ZonedWritableFile::Close() {
  return CloseInternal();
}
Status ZonedWritableFile::Flush() {
  return Status::OK();
}
Status ZonedWritableFile::Sync() {
  return DataSync();
}

void ZonedWritableFile::SetWriteLifeTimeHint(WriteLifeTimeHint hint) {
  zoneFile_->SetWriteLifeTimeHint(hint);
}
// buffer에 data를 쓴다.
Status ZonedWritableFile::BufferedWrite(const Slice& data) {
  uint32_t data_left = slice.size();
  char* data = (char*)slice.data();
  Status s;

  while (data_left) {
    uint32_t buffer_left = buffer_sz - buffer_pos;
    uint32_t to_buffer;

    if(!buffer_left) {
      s = FlushBuffer();
      if (!s.ok()) return s;
      // flush했으니깐 다 비워졌음
      buffer_left = buffer_sz;
    }

    to_buffer = data_left;
    if (to_buffer > buffer_left) {
      to_buffer = buffer_left;
    }

    memcpy(buffer + buffer_pos, data, to_buffer);
    buffer_pos += to_buffer;
    data_left -= to_buffer;
    data += to_buffer;
  }
  return Status::OK();
}
Status ZonedWritableFile::FlushBuffer() {
  Status s;

  if (buffer_pos == 0) return Status::OK();

  s = zoneFile_->BufferedAppend(buffer, buffer_pos);

  if (!s.ok()) return s;

  wp += buffer_pos;
  buffer_pos = 0;

  return Status::OK();

}

Status ZonedWritableFile::DataSync() {
  if (buffered) {
    Status s;
    buffer_mtx_.lock();
    s = FlushBuffer();
    buffer_mtx_.unlock();
    if(!s.ok()) return s;
    return zoneFile_->PersistMetadata();
  } else {
    zoneFile_->PushExtent();
  }

  return Status::OK();
}

Status ZonedWritableFile::CloseInternal() {
  if (!open) return Status::OK();
  Status s = DataSync();
  if (!s.ok()) return s;
  s = zoneFile_->CloseWR();
  if(!s.ok()) return s;

  open = false;
  return s;
}

Status ZonedSequentialFile::Read(size_t n, Slice* result, char* scratch) {
  Status s;
  
  s = zoneFile_->PositionedRead(rp, n, result, scratch, direct_);
  if (s.ok()) rp += result->size();
  return s;
}

Status ZonedSequentialFile::Skip(uint64_t n) {
  if(rp + n >= zoneFile_->GetFileSize()) return Status::InvalidArgument("Skip beyond end of file");
  rp += n;
  return Status::OK();
}

Status ZonedRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
  return zoneFile_->PositionedRead(offset, n, result, scratch, direct_);
}

Status ZoneFile::MigrateData(uint64_t offset, uint32_t length, Zone* target_zone) {
  uint32_t step = 128 << 10;
  uint32_t read_sz = step;
  int block_sz = zbd_->GetBlockSize();

  assert(offset % block_sz == 0);
  if (offset % block_sz != 0) return Status::IOError("igrateData offset is not aligned!\n");

  char* buf;
  int ret = posix_memalign((void**)&buf, block_sz, step);
  if (ret) {
    return Status::IOError("failed allocating alignment write buffer\n");
  }

  int pad_sz = 0;
  while (length > 0) {
    read_sz = length > read_sz ? read_sz : length;
    pad_sz = read_sz % block_sz == 0 ? 0 : (block_sz - (read_sz % block_sz));

    int r = zbd_->Read(buf, offset, read_sz + pad_sz, true);
    if (r < 0) {
      free(buf);
      return Status::IOError(stderror(errno));
    }

    target_zone->Append(buf, r);
    length -= read_sz;
    offset += r;
  }

  free(buf);

  return Status::OK();
}


}