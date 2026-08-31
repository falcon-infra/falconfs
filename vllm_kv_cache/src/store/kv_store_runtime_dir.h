/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * v6.5 P0: per-user runtime directory for falcon_kv_store SHM descriptors (impl P2+).
 */
#pragma once

#include <string>

namespace falcon {
namespace kv {

/** Default: /run/user/<uid>/falcon_kv_store/ when XDG_RUNTIME_DIR unset. */
std::string DefaultKVStoreRuntimeDir();

} // namespace kv
} // namespace falcon
