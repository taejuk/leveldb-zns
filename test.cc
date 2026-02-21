#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <chrono>
#include <atomic>

using namespace std;
using namespace std::chrono;

const int NUM_THREADS = 8;
const int TOTAL_WRITES = 100000; // 스레드당 쓰기 횟수
const int IO_DELAY_US = 10;      // 시뮬레이션된 I/O 지연 시간 (10마이크로초)

// --- 공통 변수 ---
uint64_t zone_wp = 0;
mutex zone_mutex;
atomic<int> completed_threads(0);

// 실제 SSD I/O를 흉내내는 함수
void SimulatedIO() {
    auto start = high_resolution_clock::now();
    while (duration_cast<microseconds>(high_resolution_clock::now() - start).count() < IO_DELAY_US) {
        // CPU를 점유하며 I/O 작업 수행 시뮬레이션
    }
}

// 1. Host-Managed 방식 (현재 방식)
// 호스트가 WP를 관리하므로, 쓰기 순서를 보장하기 위해 I/O 전체 구간에 락을 걸어야 함
void HostManagedWrite(int id) {
    for (int i = 0; i < TOTAL_WRITES; ++i) {
        lock_guard<mutex> lock(zone_mutex); // 🔒 강력한 락 (병목 지점)
        
        // 1. WP 확인
        uint64_t my_offset = zone_wp;
        
        // 2. 물리적 쓰기 수행 (ZNS는 순차 쓰기가 필수이므로 락 안에서 수행)
        SimulatedIO(); 
        
        // 3. WP 업데이트
        zone_wp += 4096;
    }
}

// 2. SSD-Managed 방식 (Zone Append 도입 시)
// SSD가 내부에서 WP를 관리하므로, 호스트는 락 없이 명령을 쏟아부을 수 있음
void DeviceManagedAppend(int id) {
    for (int i = 0; i < TOTAL_WRITES; ++i) {
        // 🔒 락 없음! (하드웨어 큐에 바로 던짐)
        
        // 1. Append 명령 전송 (하드웨어 내부에서 순차 처리)
        SimulatedIO(); 
        
        // 2. 하드웨어가 "여기에 썼다"라고 알려준 주소를 받아옴
        // (실제로는 CQE 응답을 받지만 여기서는 생략)
        uint64_t actual_offset = 0; 
    }
}

int main() {
    cout << "=== ZNS Write Performance Comparison (4 Threads) ===" << endl;
    cout << "Each thread performing " << TOTAL_WRITES << " writes." << endl;

    // --- 테스트 1: Host-Managed (현재 방식) ---
    zone_wp = 0;
    auto start1 = high_resolution_clock::now();
    vector<thread> threads1;
    for (int i = 0; i < NUM_THREADS; ++i) threads1.emplace_back(HostManagedWrite, i);
    for (auto& t : threads1) t.join();
    auto end1 = high_resolution_clock::now();
    
    double duration1 = duration_cast<milliseconds>(end1 - start1).count() / 1000.0;

    // --- 테스트 2: Zone Append (기대 방식) ---
    auto start2 = high_resolution_clock::now();
    vector<thread> threads2;
    for (int i = 0; i < NUM_THREADS; ++i) threads2.emplace_back(DeviceManagedAppend, i);
    for (auto& t : threads2) t.join();
    auto end2 = high_resolution_clock::now();
    
    double duration2 = duration_cast<milliseconds>(end2 - start2).count() / 1000.0;

    // --- 결과 출력 ---
    cout << "\n[1] Host-Managed (With Lock): " << duration1 << "s" << endl;
    cout << "[2] Zone Append (Lock-Free):  " << duration2 << "s" << endl;
    cout << "------------------------------------------------" << endl;
    cout << "Improvement: " << (duration1 / duration2) << "x faster!" << endl;

    return 0;
}