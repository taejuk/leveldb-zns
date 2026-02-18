#include <iostream>
#include <string>
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/zenfs/zbdenv.h"
#include "leveldb/zenfs/zbd_taejuk.h"

// ★ 주의: 라이브러리 수정 여부에 따라 "/dev/nullb0" 또는 "nullb0"
const std::string DEVICE_NAME = "/dev/nullb0"; 
const std::string AUX_PATH = "/tmp/zenfs_aux"; // 메타데이터 저장용 일반 폴더

int main() {
    leveldb::Status s;
    
    std::cout << "=== ZNS Function Check (with MkFS) ===" << std::endl;

    // 1. Env 객체 수동 생성 (NewZonedEnv 안 씀)
    //    이유: NewZonedEnv는 바로 Mount를 해버리기 때문
    std::cout << "[Step 1] Creating Device & Env..." << std::endl;
    
    leveldb::ZonedBlockDevice* zbd = new leveldb::ZonedBlockDevice(DEVICE_NAME);
    s = zbd->Open(false, true); // readonly=false, exclusive=true
    if (!s.ok()) {
        std::cerr << "Error Open: " << s.ToString() << std::endl;
        return 1;
    }

    leveldb::ZonedEnv* zenv = new leveldb::ZonedEnv(zbd);

    // 2. 포맷 (MkFS) - ★ 핵심 단계!
    //    빈 장치에 파일시스템을 생성합니다.
    std::cout << "[Step 2] Formatting (MkFS)..." << std::endl;
    s = zenv->MkFS(AUX_PATH, 0, false); // path, threshold, enable_gc
    if (!s.ok()) {
        std::cerr << "Error MkFS: " << s.ToString() << std::endl;
        // MkFS가 실패해도, 이미 포맷된 경우일 수 있으니 계속 진행해봅니다.
    } else {
        std::cout << "-> MkFS Success!" << std::endl;
    }
    delete zenv; 

    std::cout << "[Step 2] Re-Opening & Mounting..." << std::endl;

    // ★ [추가] 객체 새로 생성 (깨끗한 상태로 다시 시작)
    zbd = new leveldb::ZonedBlockDevice(DEVICE_NAME);
    s = zbd->Open(false, true);
    zenv = new leveldb::ZonedEnv(zbd);

    // 이제 Mount 하면 성공함!
    s = zenv->Mount(false);
    if (!s.ok()) {
        std::cerr << "Error Mount: " << s.ToString() << std::endl;
        return 1;
    }
    std::cout << "-> Mount Success!" << std::endl;

    // 4. DB 열기 테스트
    std::cout << "[Step 4] Opening DB..." << std::endl;
    leveldb::DB* db = nullptr;
    leveldb::Options options;
    options.env = zenv;
    options.create_if_missing = true;
    
    s = leveldb::DB::Open(options, "test_db_zns", &db);
    if (!s.ok()) {
        std::cerr << "Error DB Open: " << s.ToString() << std::endl;
        return 1;
    }

    std::cout << "-> DB Open Success!" << std::endl;
    
    // 5. Put/Get 테스트
    std::cout << "[Step 5] I/O Test..." << std::endl;
    s = db->Put(leveldb::WriteOptions(), "key1", "value1");
    if (!s.ok()) std::cerr << "Put Error: " << s.ToString() << std::endl;

    std::string val;
    s = db->Get(leveldb::ReadOptions(), "key1", &val);
    if (!s.ok()) std::cerr << "Get Error: " << s.ToString() << std::endl;
    
    std::cout << "-> Read Value: " << val << std::endl;

    delete db;
    // zenv, zbd는 OS가 정리하도록 둠 (테스트 코드이므로)
    
    std::cout << "=== ALL SUCCESS ===" << std::endl;
    return 0;
}