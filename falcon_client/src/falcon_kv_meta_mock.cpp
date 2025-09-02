#include "falcon_kv_meta_mock.h"

#include <map>
#include <string>
#include <random>
#include <iostream>

// 模拟元数据存储
static std::map<std::string, FormData_kv_index> g_meta_storage;

// 失败概率
static double g_meta_put_fail_rate = 0.0;
static double g_meta_get_fail_rate = 0.0;
static double g_meta_del_fail_rate = 0.0;

// 随机数生成
static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_real_distribution<double> dis(0.0, 1.0);

int FalconPut(FormData_kv_index &kv_index){
    
    std::string key = kv_index.key;

    bool is_failed = (dis(gen) < g_meta_put_fail_rate);
    if (!is_failed) {
        g_meta_storage[key] = kv_index;
        return 0;
    }else {
        return -1;
    }
}

int FalconGet(FormData_kv_index &kv_index){

    std::string key = kv_index.key;

    auto it = g_meta_storage.find(key);

    if (it == g_meta_storage.end()){
        std::cout << "Failed to find key=" << key << " in g_meta_storage" << std::endl;
        return -1;
    }

    bool is_failed = (dis(gen) < g_meta_get_fail_rate);
    if (!is_failed) {
        kv_index = g_meta_storage[key];
        return 0;
    }else {
        std::cout << "Failed to get metadata of key=" << key << " with prob";
        return -1;
    }
}

int FalconDelete(const std::string &key){

    auto it = g_meta_storage.find(key);

    if (it == g_meta_storage.end()){
        return -1;
    }

    bool is_failed = (dis(gen) < g_meta_del_fail_rate);
    if (!is_failed) {
        std::cout << "Delete metadata of key=" << key << std::endl;
        g_meta_storage.erase(it);
    }else {
        std::cout << "Failed to delete metadata of key=" << key << " with prob";
        return -1;
    }

    return 0;
}

std::pair<uint64_t, uint64_t> sliceKeyRangeFetch(uint32_t count) {
    static std::mutex fetch_mtx;
    static int global_key = 0;

    std::lock_guard<std::mutex> lock(fetch_mtx);
    int start = global_key;
    global_key += count;
    return {start, start + count - 1};
}