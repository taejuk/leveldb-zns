#include "leveldb/zenfs/zbdlib_taejuk.h"

#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <linux/fs.h> 
#include <sys/ioctl.h>
#include <fstream>
#include <string>
#include <iostream>
#include "leveldb/env.h"
#include "leveldb/status.h"

namespace leveldb {

ZbdlibBackend::ZbdlibBackend(std::string bdevname)
    : filename_(bdevname),
      read_f_(-1),
      read_direct_f_(-1),
      write_f_(-1) {}

ZbdlibBackend::~ZbdlibBackend() {
  if (read_f_ >= 0) zbd_close(read_f_);
  if (read_direct_f_ >= 0) zbd_close(read_direct_f_);
  if (write_f_ >= 0) zbd_close(write_f_);
    
}

std::string ZbdlibBackend::ErrorToString(int err) {
  char *err_str = strerror(err);
  if (err_str != nullptr) return std::string(err_str);
  return "";
}

Status ZbdlibBackend::CheckScheduler() {
  std::ostringstream path;
  std::string s = filename_;
  std::fstream f;

  s.erase(0,5);
  path << "/sys/block/" << s << "/queue/scheduler";
  f.open(path.str(), std::fstream::in);
  if (!f.is_open()) {
    return Status::InvalidArgument("Failed to open " + path.str());
  }

  std::string buf;
  getline(f, buf);

  if(buf.find("[mq-deadline]") == std::string::npos) {
    f.close();
    return Status::InvalidArgument("Current ZBD scheduler is not mq-deadline");
  }

  f.close();
  return Status::OK();
}

Status ZbdlibBackend::Open(bool readonly, bool exclusive,
                           unsigned int *max_active_zones,
                           unsigned int *max_open_zones) {
  zbd_info info;
  //std::cout << "hello " << filename_ << std::endl;
  if (exclusive) {
    read_f_ = zbd_open(filename_.c_str(), O_RDONLY | O_EXCL, &info);
  } else {
    read_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  }

  if (read_f_ < 0) {
    return Status::InvalidArgument(
      "Failed to open zbd for read: " + ErrorToString(errno)
    );
  }

  //read_direct_f_ = zbd_open(filename_.c_str(), O_RDONLY | O_DIRECT, &info);
  read_direct_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  if (read_direct_f_ < 0) {
    return Status::InvalidArgument(
      "Failed to open zoned block device for direct read: " +
        ErrorToString(errno)
    );
  }

  if(readonly) {
    write_f_ = -1;
  } else {
    write_f_ = zbd_open(filename_.c_str(), O_WRONLY | O_DIRECT, &info);
    if(write_f_ < 0) {
      return Status::InvalidArgument(
        "Failed to open zoned block device for write: " +
          ErrorToString(errno)
      );
    }
  }

  

  if (info.model != ZBD_DM_HOST_MANAGED) {
    return Status::NotSupported("Not a host managed block device.");
  }

  Status ios = CheckScheduler();
  if (ios != Status::OK()) return ios;

  block_sz_ = info.pblock_size;
  zone_sz_ = info.zone_size;
  nr_zones_ = info.nr_zones;
  *max_active_zones = info.max_nr_active_zones;
  *max_open_zones = info.max_nr_open_zones;
  return Status::OK();
}

std::unique_ptr<ZoneList> ZbdlibBackend::ListZones() {
  int ret;
  void *zones;
  unsigned int nr_zones;

  ret = zbd_list_zones(read_f_, 0, zone_sz_ * nr_zones_, ZBD_RO_ALL,
                       (struct zbd_zone**)&zones, &nr_zones);
  if (ret) {
    return nullptr;
  }

  std::unique_ptr<ZoneList> zl(new ZoneList(zones, nr_zones));

  return zl;
}

Status ZbdlibBackend::Reset(uint64_t start, bool *offline,
                              uint64_t *max_capacity) {
  unsigned int report = 1;
  struct zbd_zone z;
  int ret;

  ret = zbd_reset_zones(write_f_, start, zone_sz_);
  if(ret) return Status::IOError("Zone reset failed\n");

  ret = zbd_report_zones(read_f_, start, zone_sz_, ZBD_RO_ALL, &z, &report);

  if (ret || (report != 1)) return Status::IOError("Zone report failed\n");

  if (zbd_zone_offline(&z)) {
    *offline = true;
    *max_capacity = 0;
  } else {
    *offline = false;
    *max_capacity = zbd_zone_capacity(&z);
  }

  return Status::OK();
}
// full이 아니여도 강제로 full로 만든다.
Status ZbdlibBackend::Finish(uint64_t start) {
  int ret;

  ret = zbd_finish_zones(write_f_, start, zone_sz_);
  if (ret) return Status::IOError("Zone finish failed\n");

  return Status::OK();
}

Status ZbdlibBackend::Close(uint64_t start) {
  int ret;
  
  ret = zbd_close_zones(write_f_, start, zone_sz_);
  //std::cout << "Close ret: " << strerror(errno) << std::endl;
  //if (ret) return Status::IOError("Zone close failed\n");
  
  return Status::OK();
}
// 당분간은 이 파일은 안 읽는다고 알려서 캐시에서 없앤다.
int ZbdlibBackend::InvalidateCache(uint64_t pos, uint64_t size) {
  return posix_fadvise(read_f_, pos, size, POSIX_FADV_DONTNEED);
  //return ioctl(read_f_, BLKFLSBUF, 0);
}

int ZbdlibBackend::Read(char *buf, int size, uint64_t pos, bool direct) {
  return pread(direct ? read_direct_f_ : read_f_, buf, size, pos);
}

int ZbdlibBackend::Write(char *data, uint32_t size, uint64_t pos) {
  return pwrite(write_f_, data, size, pos);
}

}