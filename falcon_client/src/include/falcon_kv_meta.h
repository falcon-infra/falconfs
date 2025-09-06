#pragma once

#include <stdint.h>
#include <memory>
#include <vector>
#include <string>

struct FormData_Slice
{
    uint64_t sliceKey; // 分布式系统中唯一的 slice key
    uint32_t size;     // slice 大小，字节
    uint64_t location; // bio 存储返回的位置
};

struct FormData_kv_index
{
    std::string key;
    uint32_t valueLen; // 原始 value 大小，字节
    uint16_t sliceNum; // slice 数量
    std::vector<FormData_Slice> slicesMeta;
};

int FalconPut(FormData_kv_index &kv_index);

int FalconGet(FormData_kv_index &kv_index);

int FalconDelete(std::string &key);

std::pair<uint64_t, uint64_t> sliceKeyRangeFetch(uint32_t count);