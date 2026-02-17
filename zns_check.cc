#include <iostream>
#include <string>
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "leveldb/zenfs/zbdenv.h" 

const std::string DEVICE_NAME = "/dev/nvme0n1"; 
const std::string DB_PATH = "zns_test_db";

int main() {
    leveldb::Env* zns_env = nullptr;
    leveldb::DB* db = nullptr;
    leveldb::Options options;
    leveldb::Status s;

    std::cout << "========================================" << std::endl;
    std::cout << "   LevelDB-ZNS Functional Check Tool    " << std::endl;
    std::cout << "========================================" << std::endl;

    std::cout << "[Step 1] Initializing ZonedEnv on " << DEVICE_NAME << "..." << std::endl;
    s = leveldb::NewZonedEnv(&zns_env, DEVICE_NAME);
    if (!s.ok()) {
        std::cerr << "ERROR: Failed to create ZonedEnv: " << s.ToString() << std::endl;
        return 1;
    }
    std::cout << "-> Success!" << std::endl;

    options.env = zns_env;
    options.create_if_missing = true;

    std::cout << "[Step 2] Opening DB at '" << DB_PATH << "'..." << std::endl;
    s = leveldb::DB::Open(options, DB_PATH, &db);
    if (!s.ok()) {
        std::cerr << "ERROR: Failed to open DB: " << s.ToString() << std::endl;
        return 1;
    }
    std::cout << "-> Success! DB Pointer: " << db << std::endl;

    std::cout << "[Step 3] Writing Data (Key: hello, Value: world)..." << std::endl;
    s = db->Put(leveldb::WriteOptions(), "hello", "world");
    if (!s.ok()) {
        std::cerr << "ERROR: Put failed: " << s.ToString() << std::endl;
        delete db; return 1;
    }
    std::cout << "-> Success!" << std::endl;

    std::cout << "[Step 4] Reading Data..." << std::endl;
    std::string value;
    s = db->Get(leveldb::ReadOptions(), "hello", &value);
    if (!s.ok()) {
        std::cerr << "ERROR: Get failed: " << s.ToString() << std::endl;
        delete db; return 1;
    }
    std::cout << "-> Success! Retrieved Value: " << value << std::endl;

    if (value != "world") {
        std::cerr << "ERROR: Data Mismatch! Expected 'world', got '" << value << "'" << std::endl;
        delete db; return 1;
    }

    delete db;
    
    std::cout << "========================================" << std::endl;
    std::cout << "   ALL CHECKS PASSED! READY FOR BENCH   " << std::endl;
    std::cout << "========================================" << std::endl;

    return 0;
}