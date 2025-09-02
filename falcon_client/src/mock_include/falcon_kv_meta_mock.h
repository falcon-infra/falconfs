#pragma once

#include "falcon_kv_server.h"

int FalconPut(FormData_kv_index &kv_index);

int FalconGet(FormData_kv_index &kv_index);

int FalconDelete(const std::string &key);

std::pair<uint64_t, uint64_t> sliceKeyRangeFetch(uint32_t count);