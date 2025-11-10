#pragma once

#include <cstdint>
#include <utility>

enum SliceIdType : uint8_t {
    KV_TYPE = 0,
    FILE_TYPE = 1
};

std::pair<uint64_t, uint64_t> sliceKeyRangeFetch(uint32_t count, SliceIdType type = KV_TYPE);