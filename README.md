# LevelDB-ZNS: Zoned Namespace Optimized Storage Engine for Blockchain Workloads

![LevelDB](https://img.shields.io/badge/LevelDB-1.23-blue)
![C++](https://img.shields.io/badge/C++-11/14/17-blue.svg)
![Platform](https://img.shields.io/badge/Platform-Linux-lightgrey)
![ZNS](https://img.shields.io/badge/Storage-ZNS_SSD-orange)

LevelDB-ZNS는 ZNS(Zoned Namespace) SSD의 하드웨어적 특성(Append-only)과 LSM-Tree의 소프트웨어적 구조를 완벽하게 결합한 고성능 스토리지 엔진입니다. 

특히, 방대한 상태 업데이트와 대규모 I/O 스파이크(Burst Writes)가 발생하는 **블록체인 노드 환경**에 최적화되어 있으며, 파일 시스템(ext4 등)을 거치지 않는 Direct I/O와 데이터 수명 주기(Lifetime) 기반의 존 할당 최적화를 통해 **하드웨어 쓰기 증폭(Device WAF)을 1.0에 가깝게 최소화**하였습니다. 구체적인 코드 분석 및 성능 비교는 zns report.pdf에 있습니다.

## ✨ Key Features

* **Native ZNS Integration (`ZonedEnv`)**
  * OS의 VFS(가상 파일 시스템) 및 저널링 계층을 완전히 우회하여, 하드웨어 장치에 직접 순차 쓰기(Sequential Write)를 수행합니다.
  * 가변 길이의 `ZoneExtent` 매핑을 통해 LSM-Tree의 논리적 파일과 ZNS의 물리적 존 크기 불일치 문제를 해결하였습니다.
* **Lifetime-based Data Placement**
  * 데이터의 수명 주기 힌트(Write Life Time Hint)를 기반으로 **단기 수명 데이터(WAL)와 장기 수명 데이터(SSTable)를 서로 다른 존에 물리적으로 분리**하여 저장합니다.
  * 가비지 컬렉션(GC) 시 발생하는 유효 데이터의 이동(Copy-back)을 원천 차단하여 쓰기 증폭(WAF)을 극적으로 낮춥니다.
* **Event-Driven Garbage Collection**
  * 기존의 단순 폴링(10초 대기) 방식 GC를 `std::condition_variable`을 활용한 **이벤트 기반 하이브리드 GC**로 재설계하였습니다.
  * 블록체인 환경의 폭발적인 I/O 유입 시 즉각적으로 GC 스레드를 기상시켜, 공간 부족(No Space) 에러 및 멈춤(Stall) 현상을 방지합니다.

## 🏗 System Architecture

<img width="843" height="560" alt="스크린샷 2026-02-21 오후 9 02 57" src="https://github.com/user-attachments/assets/51cbc352-eaf6-4a5e-94dd-1c37eb8df895" />


기존 LevelDB 엔진의 수정 없이 하단 `Env` 계층을 `ZonedEnv`로 추상화하였으며, 하드웨어 제어 로직은 `ZonedBlockDeviceBackend`로 캡슐화하여 높은 유지보수성과 확장성을 보장합니다.

## 📊 Performance Evaluation

리눅스 커널의 `null_blk` 에뮬레이터 환경에서 철저하게 변인을 통제한 상태(OS Page Cache 무효화, 1GB Disk, 64 Zones)로 진행한 벤치마크 결과입니다. (데이터 크기: 500MB)

| 워크로드 | 지표 | 기존 LevelDB (ext4) | LevelDB-ZNS (제안 시스템) | 향상률 |
| :--- | :--- | :--- | :--- | :--- |
| **무작위 쓰기 (fillrandom)** | **처리량 (MB/s)** | 135.2 MB/s | **166.8 MB/s** | **+ 23.3% 향상** |
| | **지연 시간 (µs/op)**| 7.336 µs | **5.945 µs** | **- 18.9% 감소** |
| **덮어쓰기 (overwrite)** | **처리량 (MB/s)** | 114.3 MB/s | **166.6 MB/s** | **+ 45.7% 향상** |
| | **지연 시간 (µs/op)**| 8.681 µs | **5.953 µs** | **- 31.4% 감소** |
| **종합 지표** | **Device WAF** | 측정 불가 (저널링 부하 큼) | **1.001** | **하드웨어 수준 WAF 완벽 억제** |

> **분석:** ZNS 기반 시스템은 덮어쓰기(Overwrite) 환경에서도 성능 하락 없이 166MB/s의 균일한 처리량을 보였으며, 불필요한 디스크 I/O를 소거하여 WAF 1.001이라는 획기적인 수치를 달성했습니다.


## 🛠 Getting Started

### Prerequisites
* Linux Kernel 5.10+ (Zoned Block Device 지원 필요)
* C++ 17 호환 컴파일러 (GCC / Clang)
* CMake

### 1. ZNS Emulator Setup (`null_blk`)
가상의 ZNS 환경을 구성하기 위해 리눅스 `null_blk` 모듈을 사용합니다. (1GB 용량, 16MB Zone Size 64개 구성)

```bash
sudo mkdir -p /sys/kernel/config/nullb/nullb0
echo 1024 | sudo tee /sys/kernel/config/nullb/nullb0/size
echo 1 | sudo tee /sys/kernel/config/nullb/nullb0/memory_backed
echo 1 | sudo tee /sys/kernel/config/nullb/nullb0/zoned
echo 16 | sudo tee /sys/kernel/config/nullb/nullb0/zone_size
echo 1 | sudo tee /sys/kernel/config/nullb/nullb0/power
echo mq-deadline | sudo tee /sys/block/nullb0/queue/scheduler
```

### 2. Build
```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

### 3. Run Benchmark
```bash
# OS 캐시 초기화 (정확한 성능 측정을 위함)
sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches

# 벤치마크 실행 (ZNS 디바이스 타겟)
./db_bench_zns --zbd=/dev/nullb0 --benchmarks="fillrandom,stats" --value_size=1024 --num=500000
```

### 4. Future Work
Zone Append 최적화: ZNS 하드웨어 컨트롤러의 Zone Append 명령어를 활용한 Lock-free 멀티스레딩 구현
