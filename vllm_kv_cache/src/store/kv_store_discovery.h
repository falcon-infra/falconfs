/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * v6.5 P0: declarations for CN-driven KV store discovery (implementation in P4+).
 */
#pragma once

#include <string>

namespace falcon {
namespace kv {

/** Resolves BRPC endpoints for store_node_id rows (stub until P4). */
struct KVStoreDiscoveryOptions {
    std::string cn_conninfo;
};

} // namespace kv
} // namespace falcon
