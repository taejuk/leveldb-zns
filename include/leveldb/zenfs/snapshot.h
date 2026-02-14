#pragma once

#include <string>
#include <vector>

#include "leveldb/zenfs/io_taejuk.h"
#include "leveldb/zenfs/zbd_taejuk.h"

namespace leveldb {

struct TaejukSnapshotOptions {
  bool zbd_ = 0;
  bool zone_ = 0;
  bool zone_file_ = 0;
  bool trigger_report_ = 0;
  bool log_garbage_ = 0;
  bool as_lock_free_as_possible_ = 1;
};

class ZBDSnapshot {
 public:
  uint64_t free_space;
  uint64_t used_space;
  uint64_t reclaimable_space;

 public:
  ZBDSnapshot() = default;
  ZBDSnapshot(const ZBDSnapshot&) = default;
  ZBDSnapshot(ZonedBlockDevice& zbd)
      : free_space(zbd.GetFreeSpace()),
        used_space(zbd.GetUsedSpace()),
        reclaimable_space(zbd.GetReclaimableSpace()) {}
};

class ZoneSnapshot {
 public:
  uint64_t start;
  uint64_t wp;

  uint64_t capacity;
  uint64_t used_capacity;
  uint64_t max_capacity;

 public:
  ZoneSnapshot(const Zone& zone)
      : start(zone.start_),
        wp(zone.wp_),
        capacity(zone.capacity_),
        used_capacity(zone.used_capacity_),
        max_capacity(zone.max_capacity_) {}
};

class ZoneExtentSnapshot {
 public:
  uint64_t start;
  uint64_t length;
  uint64_t zone_start;
  std::string filename;

 public:
  ZoneExtentSnapshot(const ZoneExtent& extent, const std::string fname)
      : start(extent.start_),
        length(extent.length_),
        zone_start(extent.zone_->start_),
        filename(fname) {}
};

class ZoneFileSnapshot {
 public:
  uint64_t file_id;
  std::string filename;
  std::vector<ZoneExtentSnapshot> extents;

 public:
  ZoneFileSnapshot(ZoneFile& file)
      : file_id(file.GetID()), filename(file.GetFilename()) {
    for (const auto* extent : file.GetExtents()) {
      extents.emplace_back(*extent, filename);
    }
  }
};

class TaejukSnapshot {
 public:
  TaejukSnapshot() {}

  TaejukSnapshot& operator=(TaejukSnapshot&& snapshot) {
    zbd_ = snapshot.zbd_;
    zones_ = std::move(snapshot.zones_);
    zone_files_ = std::move(snapshot.zone_files_);
    extents_ = std::move(snapshot.extents_);
    return *this;
  }

 public:
  ZBDSnapshot zbd_;
  std::vector<ZoneSnapshot> zones_;
  std::vector<ZoneFileSnapshot> zone_files_;
  std::vector<ZoneExtentSnapshot> extents_;
};
}