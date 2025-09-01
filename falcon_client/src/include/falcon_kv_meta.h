#pragma once

#include <stdint.h>
#include <memory>
#include <vector>
#include <string>

#include "router.h"
#include "connection.h"

extern std::shared_ptr<Router> router;

int FalconPut(FormData_kv_index &kv_index);

int FalconGet(FormData_kv_index &kv_index);

int FalconDelete(std::string &key);