#include "leveldb/zenfs/zbd_taejuk.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/zenfs/snapshot.h"
#include "leveldb/zenfs/zbdlib_taejuk.h"

#define KB (1024)
#define MB (1024 * KB)

#define TAEJUK_META_ZONES (3)
#define TAEJUK_MIN_ZONES (32)

namespace leveldb {

Zone::Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
           std::unique_ptr<ZoneList> &zones, unsigned int idx)
    : zbd_(zbd),
      zbd_be_(zbd_be),
      busy_(false),
      start_(zbd_be->ZoneStart(zones, idx)),
      max_capacity_(zbd_be->ZoneMaxCapacity(zones, idx)),
      wp_(zbd_be->ZoneWp(zones, idx)) {
  lifetime_ = WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  if (zbd_be->ZoneIsWritable(zones, idx))
    capacity_ = max_capacity_ - (wp_ - start_);
}

bool Zone::IsUsed() { return (used_capacity_ > 0); }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

void Zone::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"capacity\":" << capacity_ << ",";
  json_stream << "\"max_capacity\":" << max_capacity_ << ",";
  json_stream << "\"wp\":" << wp_ << ",";
  json_stream << "\"lifetime\":" << lifetime_ << ",";
  json_stream << "\"used_capacity\":" << used_capacity_;
  json_stream << "}";
}

Status Zone::Reset() {
  bool offline;
  uint64_t max_capacity;

  assert(!IsUsed());
  assert(IsBusy());

  Status ios = zbd_be_->Reset(start_, &offline, &max_capacity);
  if(ios != Status::OK()) return ios;
  // offline이면 하드웨어 결함으로 인해 더이상 못쓴다.
  if(offline) capacity_ = 0;
  else max_capacity_ = capacity_ = max_capacity;
  
  wp_ = start_;
  lifetime_ = WLTH_NOT_SET;

  return Status::OK();
}

Status Zone::Finish() {
  assert(IsBusy());

  Status ios = zbd_be_->Finish(start_);
  if (ios != Status::OK()) return ios;

  capacity_ = 0;
  wp_ = start_ + zbd_->GetZoneSize();

  return Status::OK();
}

Status Zone::Close() {
  assert(IsBusy());

  if (!(IsEmpty() || IsFull())) {
    Status ios = zbd_be_->Close(start_);
    if (ios != Status::OK()) return ios;
  }

  return Status::OK();
}

Status Zone::Append(char *data, uint32_t size) {
  
  char *ptr = data;
  uint32_t left = size;
  int ret;

  if(capacity_ < size) return Status::NoSpace("Not enough capacity for append");

  assert((size % zbd_->GetBlockSize()) == 0);

  while (left) {
    ret = zbd_be_->Write(ptr, left, wp_);
    if(ret < 0) {
      return Status::IOError(strerror(errno));
    }

    ptr += ret;
    wp_ += ret;
    capacity_ -= ret;
    left -= ret;
    zbd_->AddBytesWritten(ret);
  }

  return Status::OK();
}

inline Status Zone::CheckRelease() {
  if(!Release()) {
    assert(false);
    return Status::Corruption("Failed to unset busy flag of zone " +
                                std::to_string(GetZoneNr()));
  }

  return Status::OK();
}

Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for(const auto z : io_zones)
    if(z->start_ <= offset && offset < (z->start_ + zbd_be_->GetZoneSize())) return z;
  return nullptr;
}

ZonedBlockDevice::ZonedBlockDevice(std::string path, ZbdBackendType backend, std::shared_ptr<Logger> logger) : logger_(logger)
{
  zbd_be_ = std::unique_ptr<ZbdlibBackend>(new ZbdlibBackend(path));
}

ZonedBlockDevice::ZonedBlockDevice(std::string path) : logger_(nullptr) 
{
  zbd_be_ = std::unique_ptr<ZbdlibBackend>(new ZbdlibBackend(path));
}


Status ZonedBlockDevice::Open(bool readonly, bool exclusive) {
  std::unique_ptr<ZoneList> zone_rep;
  unsigned int max_nr_active_zones;
  unsigned int max_nr_open_zones;
  Status s;
  uint64_t i = 0;
  uint64_t m = 0;

  int reserved_zones = 2;

  if(!readonly && !exclusive) return Status::InvalidArgument("Write opens must be exclusive");

  Status ios = zbd_be_->Open(readonly, exclusive, &max_nr_active_zones, &max_nr_open_zones);
  if (ios != Status::OK()) return ios;
  
  if (zbd_be_->GetNrZones() < TAEJUK_MIN_ZONES) return Status::NotSupported("To few zones on zoned backend (" +std::to_string(TAEJUK_MIN_ZONES) +" required)");

  if (max_nr_active_zones == 0) max_nr_active_io_zones_ = zbd_be_->GetNrZones();
  else max_nr_active_io_zones_ = max_nr_active_zones - reserved_zones;

  if (max_nr_open_zones == 0) max_nr_open_io_zones_ = zbd_be_->GetNrZones();
  else max_nr_open_io_zones_ = max_nr_open_zones - reserved_zones;

  zone_rep = zbd_be_->ListZones();
  if (zone_rep == nullptr || zone_rep->ZoneCount() != zbd_be_->GetNrZones()) {
    // Error(logger_, "Failed to list zones");
    return Status::IOError("Failed to list zones");
  }

  while (m < TAEJUK_META_ZONES && i < zone_rep->ZoneCount()) {
    if (zbd_be_->ZoneIsSwr(zone_rep, i)) {
      if(!zbd_be_->ZoneIsOffline(zone_rep, i)) {
        meta_zones.push_back(new Zone(this, zbd_be_.get(), zone_rep, i));
      }
      m++;
    }
    i++;
  }

  active_io_zones_ = 0;
  open_io_zones_ = 0;

  for(; i < zone_rep->ZoneCount(); i++) {
    if(zbd_be_->ZoneIsSwr(zone_rep, i)) {
      if(!zbd_be_->ZoneIsOffline(zone_rep, i)) {
        Zone *newZone = new Zone(this, zbd_be_.get(), zone_rep, i);
        if (!newZone->Acquire()) {
          assert(false);
          return Status::Corruption("Failed to set busy flag of zone " +
                                      std::to_string(newZone->GetZoneNr()));
        }
        io_zones.push_back(newZone);
        if (zbd_be_->ZoneIsActive(zone_rep, i)) {
          active_io_zones_++;
          if (zbd_be_->ZoneIsOpen(zone_rep, i)) {
            if (!readonly) {
              newZone->Close();
            }
          }
        }
        Status status = newZone->CheckRelease();
        if(!status.ok()) return status;
      }
    }
  }

  start_time_ = time(NULL);

  return Status::OK();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
  }

  return free;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
  uint64_t used = 0;
  for (const auto z : io_zones) {
    used += z->used_capacity_;
  }
  return used;
}

uint64_t ZonedBlockDevice::GetReclaimableSpace() {
  uint64_t reclaimable = 0;
  for (const auto z : io_zones) {
    if (z->IsFull()) reclaimable += (z->max_capacity_ - z->used_capacity_);
  }
  return reclaimable;
}

void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;

  for (const auto z : io_zones) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  // Info(logger_,
  //      "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
  //      "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
  //      "%lu %lu %lu %lu %ld %ld\n",
  //      time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
  //      100 * reclaimable_capacity / reclaimables_max_capacity, active,
  //      active_io_zones_.load(), open_io_zones_.load());
}

void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      // Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
      //       z->start_, used, used / MB);
    }
  }
}

void ZonedBlockDevice::LogGarbageInfo() {
  // Log zone garbage stats vector.
  //
  // The values in the vector represents how many zones with target garbage
  // percent. Garbage percent of each index: [0%, <10%, < 20%, ... <100%, 100%]
  // For example `[100, 1, 2, 3....]` means 100 zones are empty, 1 zone has less
  // than 10% garbage, 2 zones have  10% ~ 20% garbage ect.
  //
  // We don't need to lock io_zones since we only read data and we don't need
  // the result to be precise.
  int zone_gc_stat[12] = {0};
  for (auto z : io_zones) {
    if (!z->Acquire()) {
      continue;
    }

    if (z->IsEmpty()) {
      zone_gc_stat[0]++;
      z->Release();
      continue;
    }

    double garbage_rate = 0;
    if (z->IsFull()) {
      garbage_rate =
          double(z->max_capacity_ - z->used_capacity_) / z->max_capacity_;
    } else {
      garbage_rate =
          double(z->wp_ - z->start_ - z->used_capacity_) / z->max_capacity_;
    }
    assert(garbage_rate >= 0);
    int idx = int((garbage_rate + 0.1) * 10);
    zone_gc_stat[idx]++;

    z->Release();
  }

  std::stringstream ss;
  ss << "Zone Garbage Stats: [";
  for (int i = 0; i < 12; i++) {
    ss << zone_gc_stat[i] << " ";
  }
  ss << "]";
  // Info(logger_, "%s", ss.str().data());
}

ZonedBlockDevice::~ZonedBlockDevice() {
  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }
}

#define LIFETIME_DIFF_NOT_GOOD (100)
#define LIFETIME_DIFF_COULD_BE_WORSE (50)

unsigned int GetLifeTimeDiff(WriteLifeTimeHint zone_lifetime, WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= WLTH_EXTREME);
  if ((file_lifetime == WLTH_NOT_SET) ||
      (file_lifetime == WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;
  //if (zone_lifetime == file_lifetime) return LIFETIME_DIFF_COULD_BE_WORSE;
  if (zone_lifetime == file_lifetime) return 0;
  return LIFETIME_DIFF_NOT_GOOD;
}

Status ZonedBlockDevice::AllocateMetaZone(Zone **out_meta_zone) {
  assert(out_meta_zone);
  *out_meta_zone = nullptr;

  for (const auto z : meta_zones) {
    if (z->Acquire()) {
      if (!z->IsUsed()) {
        if (!z->IsEmpty() && !z->Reset().ok()) {
          Status status = z->CheckRelease();
          if(!status.ok()) return status;
          continue;
        }
        *out_meta_zone = z;
        return Status::OK();
      }
    }
  }
  assert(true);
  return Status::NoSpace("Out of metadata zones");
}

Status ZonedBlockDevice::ResetUnusedIOZones() {
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (!z->IsEmpty() && !z->IsUsed()) {
        bool full = z->IsFull();
        Status reset_status = z->Reset();
        Status release_status = z->CheckRelease();
        if(!reset_status.ok()) return reset_status;
        if(!release_status.ok()) return release_status;
        if(!full) PutActiveIOZoneToken();
      } else {
        Status release_status = z->CheckRelease();
        if(!release_status.ok()) return release_status;
      }
    }
  }
  return Status::OK();
}

void ZonedBlockDevice::PutActiveIOZoneToken() {
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    active_io_zones_--;
  }
  zone_resources_.notify_one();
}

void ZonedBlockDevice::WaitForOpenIOZoneToken(bool prioritized) {
  long allocator_open_limit;
  // 중요한거면 끝까지 다 쓰고
  // 안 중요하면 하나 비우겠다/.
  if(prioritized) {
    allocator_open_limit = max_nr_open_io_zones_;
  } else {
    allocator_open_limit = max_nr_open_io_zones_ - 1;
  }

  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  zone_resources_.wait(lk, [this, allocator_open_limit] {
    if (open_io_zones_.load() < allocator_open_limit) {
      open_io_zones_++;
      return true;
    } else {
      return false;
    }
  });
}

bool ZonedBlockDevice::GetActiveIOZoneTokenIfAvailable() {
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  if(active_io_zones_.load() < max_nr_active_io_zones_) {
    active_io_zones_++;
    return true;
  }
  return false;
}

void ZonedBlockDevice::PutOpenIOZoneToken() {
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    open_io_zones_--;
  }
  zone_resources_.notify_one();
}

Status ZonedBlockDevice::ApplyFinishThreshold() {
  Status s;

  if (finish_threshold_ == 0) return Status::OK();

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      bool within_finish_threshold =  z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100);
      if(!(z->IsEmpty() || z->IsFull()) && within_finish_threshold) {
        s = z->Finish();
        if (!s.ok()) {
          z->Release();
          return s;
        }
        s = z->CheckRelease();
        if (!s.ok()) return s;
        PutActiveIOZoneToken();
      } else {
        s = z->CheckRelease();
        if(!s.ok()) return s;
      }
    }
  }

  return Status::OK();
}

Status ZonedBlockDevice::FinishCheapestIOZone() {
  Status s;
  Zone *finish_victim = nullptr;

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if(z->IsEmpty() || z->IsFull()) {
        s = z->CheckRelease();
        if(!s.ok()) return s;
        continue;
      }
      if (finish_victim == nullptr) {
        finish_victim = z;
        continue;
      }
      // finish_victim에 대한 lock은 바뀌기 전까지 계속 잡고 있는다.
      if (finish_victim->capacity_ > z->capacity_) {
        s = finish_victim->CheckRelease();
        if (!s.ok()) return s;
        finish_victim = z;
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  if (finish_victim == nullptr) {
    return Status::OK();
  }

  s = finish_victim->Finish();
  Status release_status = finish_victim->CheckRelease();

  if (s.ok()) {
    PutActiveIOZoneToken();
  }

  if (!release_status.ok()) {
    return release_status;
  }

  return s;
}

Status ZonedBlockDevice::GetBestOpenZoneMatch(
    WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out, Zone **zone_out, uint32_t min_capacity
) {
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Zone *allocated_zone = nullptr;
  Status s;

  for(const auto z : io_zones) {
    if (z->Acquire()) {
      if((z->used_capacity_ > 0) && !z->IsFull() && z->capacity_ >= min_capacity) {
        unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
        if (diff <= best_diff) {
          if (allocated_zone != nullptr) {
            s = allocated_zone->CheckRelease();
            if(!s.ok()) {
              Status s_ = z->CheckRelease();
              if(!s_.ok()) return s_;
              return s;
            }
          }
          allocated_zone = z;
          best_diff = diff;
        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  *best_diff_out = best_diff;
  *zone_out = allocated_zone;

  return Status::OK();
}

Status ZonedBlockDevice::AllocateEmptyZone(Zone **zone_out) {
  Status s;
  Zone *allocated_zone = nullptr;
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (z->IsEmpty()) {
        allocated_zone = z;
        break;
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }
  *zone_out = allocated_zone;
  return Status::OK();
}

Status ZonedBlockDevice::InvalidateCache(uint64_t pos, uint64_t size) {
  int ret = zbd_be_->InvalidateCache(pos, size);

  if (ret) {
    return Status::IOError("Failed to invalidate cache");
  }
  return Status::OK();
}

int ZonedBlockDevice::Read(char *buf, uint64_t offset, int n, bool direct) {
  int ret = 0;
  int left = n;
  int r = -1;

  while (left) {
    r = zbd_be_->Read(buf, left, offset, direct);
    if(r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ret += r;
    buf += r;
    left -= r;
    offset += r;
  }

  if (r < 0) return r;
  return ret;
}

Status ZonedBlockDevice::ReleaseMigrateZone(Zone *zone) {
  Status s = Status::OK();
  {
    std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
    migrating_ = false;
    if (zone != nullptr) {
      s = zone->CheckRelease();
    }
  }
  migrate_resource_.notify_one();
  return s;
}

Status ZonedBlockDevice::TakeMigrateZone(Zone **out_zone,
                                         WriteLifeTimeHint file_lifetime,
                                         uint32_t min_capacity) {
  std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
  // 혼자 migrate 해야 한다.
  migrate_resource_.wait(lock, [this] { return !migrating_; });

  migrating_ = true;

  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  auto s =
      GetBestOpenZoneMatch(file_lifetime, &best_diff, out_zone, min_capacity);
  if (s.ok() && (*out_zone) != nullptr) {
    // Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
  } else {
    migrating_ = false;
  }

  return s;
}

Status ZonedBlockDevice::AllocateIOZone(WriteLifeTimeHint file_lifetime, IOType io_type, Zone **out_zone) {
  Zone *allocated_zone = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  int new_zone = 0;
  Status s;

  s = GetZoneDeferredStatus();
  if (!s.ok()) return s;

  if (io_type != IOType::kWAL) {
    s = ApplyFinishThreshold();
    if (!s.ok()) {
      return s;
    }
  }

  WaitForOpenIOZoneToken(io_type == IOType::kWAL);

  s = GetBestOpenZoneMatch(file_lifetime, &best_diff, &allocated_zone);
  if (!s.ok()) {
    PutOpenIOZoneToken();
    return s;
  }

  if (best_diff >= LIFETIME_DIFF_COULD_BE_WORSE) {
    bool got_token = GetActiveIOZoneTokenIfAvailable();

    if (allocated_zone != nullptr) {
      if (!got_token && best_diff == LIFETIME_DIFF_COULD_BE_WORSE) {
        // Debug(logger_,
        //       "Allocator: avoided a finish by relaxing lifetime diff "
        //       "requirement\n");
      } else {
        s = allocated_zone->CheckRelease();
        if (!s.ok()) {
          PutOpenIOZoneToken();
          if (got_token) PutActiveIOZoneToken();
          return s;
        }
        allocated_zone = nullptr;
      }
    }

    if (allocated_zone == nullptr) {
      while (!got_token && !GetActiveIOZoneTokenIfAvailable()) {
        s = FinishCheapestIOZone();
        if (!s.ok()) {
          PutOpenIOZoneToken();
          return s;
        }
      }

      s = AllocateEmptyZone(&allocated_zone);
      if (!s.ok()) {
        PutActiveIOZoneToken();
        PutOpenIOZoneToken();
        return s;
      }

      if (allocated_zone != nullptr) {
        assert(allocated_zone->IsBusy());
        allocated_zone->lifetime_ = file_lifetime;
        new_zone = true;
      } else {
        PutActiveIOZoneToken();
      }
    }
  }

  if (allocated_zone) {
    assert(allocated_zone->IsBusy());
    // Debug(logger_,
    //       "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
    //       new_zone, allocated_zone->start_, allocated_zone->wp_,
    //       allocated_zone->lifetime_, file_lifetime);
  } else {
    PutOpenIOZoneToken();
  }

  if (io_type != IOType::kWAL) {
    LogZoneStats();
  }
  *out_zone = allocated_zone;

  return Status::OK();
}

std::string ZonedBlockDevice::GetFilename() { return zbd_be_->GetFilename(); }

uint32_t ZonedBlockDevice::GetBlockSize() { return zbd_be_->GetBlockSize(); }

uint64_t ZonedBlockDevice::GetZoneSize() { return zbd_be_->GetZoneSize(); }

uint32_t ZonedBlockDevice::GetNrZones() { return zbd_be_->GetNrZones(); }

void ZonedBlockDevice::EncodeJsonZone(std::ostream &json_stream,
                                      const std::vector<Zone *> zones) {
  bool first_element = true;
  json_stream << "[";
  for (Zone *zone : zones) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    zone->EncodeJson(json_stream);
  }

  json_stream << "]";
}

void ZonedBlockDevice::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"meta\":";
  EncodeJsonZone(json_stream, meta_zones);
  json_stream << ",\"io\":";
  EncodeJsonZone(json_stream, io_zones);
  json_stream << "}";
}

Status ZonedBlockDevice::GetZoneDeferredStatus() {
  std::lock_guard<std::mutex> lock(zone_deferred_status_mutex_);
  return zone_deferred_status_;
}

void ZonedBlockDevice::SetZoneDeferredStatus(Status status) {
  std::lock_guard<std::mutex> lk(zone_deferred_status_mutex_);
  if (!zone_deferred_status_.ok()) {
    zone_deferred_status_ = status;
  }
}

void ZonedBlockDevice::GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot) {
  for (auto *zone : io_zones) {
    snapshot.emplace_back(*zone);
  }
}

}
