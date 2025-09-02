#include "bio_c.h"
#include <chrono>
#include <cstring>
#include <map>
#include <random>
#include <thread>

#include <iostream>

// 全局随机数生成器
static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_real_distribution<double> dis(0.0, 1.0);

// 本地数据存取空间
static std::map<std::string, std::string> g_storage;
static std::mutex g_storage_mtx; // 保证本地 g_storage 存储成功

// 数据存取失败概率
static double g_put_fail_rate = 0.0;
static double g_get_fail_rate = 0.0;

CResult BioCalcLocation(uint64_t tenantId, uint64_t objectId, ObjLocation *location)
{
    std::uniform_int_distribution<uint64_t> local_dis(0, 100);
    location->location[0] = local_dis(gen);
    return CResult::RET_CACHE_OK;
}

CResult BioAsyncPut(uint64_t tenantId,
                    const char *key,
                    const char *value,
                    uint64_t length,
                    ObjLocation location,
                    BioLoadCallback callback,
                    void *context)
{
    if (!key || !value || length == 0 || !callback) {
        return CResult::RET_CACHE_ERROR;
    }

    bool is_failed = (dis(gen) < g_put_fail_rate);

    std::string str_key = std::string(key);
    std::string str_value = std::string(value, length);

    // 启动异步线程
    std::thread([is_failed, callback, context, str_key, str_value]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(dis(gen) * 100)));

        if (!is_failed) {
            {
                std::lock_guard<std::mutex> lock(g_storage_mtx);
                g_storage[str_key] = str_value;
            }
        }

        int32_t status = is_failed ? 2 : 0;
        callback(context, status);
    }).detach();

    return CResult::RET_CACHE_OK;
}

CResult BioAsyncGet(uint64_t tenantId,
                    const char *key,
                    uint64_t offset,
                    uint64_t length,
                    ObjLocation location,
                    char *value,
                    BioGetCallbackFunc callback,
                    void *context)
{
    if (!key || length == 0 || !callback) {
        return CResult::RET_CACHE_ERROR;
    }

    bool is_failed = (dis(gen) < g_get_fail_rate);

    std::string str_key = std::string(key);

    std::cout << "asyncget str_key = " << str_key << " is_failed = " << is_failed << std::endl;

    std::thread([&is_failed, str_key, value, callback, context]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(dis(gen) * 100)));
        uint32_t valLength = 0;
        if (!is_failed) {
            {
                std::lock_guard<std::mutex> lock(g_storage_mtx);
                if (auto it = g_storage.find(str_key); it != g_storage.end()) {
                    const std::string str_value = it->second;
                    valLength = str_value.size();
                    memcpy(value, str_value.data(), valLength);
                } else {
                    is_failed = true;
                }
            }
        }
        int32_t status = is_failed ? 2 : 0;
        callback(context, status, valLength);
    }).detach();

    return CResult::RET_CACHE_OK;
}

CResult BioDelete(uint64_t tenantId, const char *key, ObjLocation location)
{
    if (!key) {
        return CResult::RET_CACHE_ERROR;
    }

    {
        std::lock_guard<std::mutex> lock(g_storage_mtx);
        auto it = g_storage.find(std::string(key));
        if (it != g_storage.end()) {
            std::cout << "Delete data of key=" << std::string(key) << std::endl;
            g_storage.erase(it);
        }
    }

    return CResult::RET_CACHE_OK;
}