/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include "property_key.h"

class FalconPropertyKey : public PropertyKey {
  public:
    inline static const auto FALCON_LOG_DIR =
        PropertyKey::Builder("main", "falcon_log_dir", FALCON, FALCON_STRING).build();

    inline static const auto FALCON_LOG_LEVEL =
        PropertyKey::Builder("main", "falcon_log_level", FALCON, FALCON_STRING).build();

    inline static const auto FALCON_LOG_MAX_SIZE_MB =
        PropertyKey::Builder("main", "falcon_log_max_size_mb", FALCON, FALCON_UINT).build();

    inline static const auto FALCON_THREAD_NUM =
        PropertyKey::Builder("main", "falcon_thread_num", FALCON, FALCON_UINT).build();

    inline static const auto FALCON_NODE_ID =
        PropertyKey::Builder("main", "falcon_node_id", FALCON, FALCON_UINT).build();

    inline static const auto FALCON_CACHE_ROOT =
        PropertyKey::Builder("main", "falcon_cache_root", FALCON, FALCON_STRING).build();

    inline static const auto FALCON_DIR_NUM =
        PropertyKey::Builder("main", "falcon_dir_num", FALCON, FALCON_UINT).build();

    inline static const auto FALCON_BLOCK_SIZE =
        PropertyKey::Builder("main", "falcon_block_size", FALCON, FALCON_UINT).build();

    inline static const auto FALCON_BIG_FILE_READ_SIZE =
        PropertyKey::Builder("main", "falcon_read_big_file_size", FALCON, FALCON_UINT).build();

    inline static const auto FALCON_CLUSTER_VIEW =
        PropertyKey::Builder("main", "falcon_cluster_view", FALCON, FALCON_ARRAY).build();

    inline static const auto FALCON_SERVER_IP =
        PropertyKey::Builder("main", "falcon_server_ip", FALCON, FALCON_STRING).build();

    inline static const auto FALCON_SERVER_PORT =
        PropertyKey::Builder("main", "falcon_server_port", FALCON, FALCON_STRING).build();

    inline static const auto FALCON_ASYNC = PropertyKey::Builder("main", "falcon_async", FALCON, FALCON_BOOL).build();

    inline static const auto FALCON_PERSIST =
        PropertyKey::Builder("main", "falcon_persist", FALCON, FALCON_BOOL).build();

    inline static const auto FALCON_PRE_BLOCKNUM =
        PropertyKey::Builder("main", "falcon_preblock_num", FALCON, FALCON_UINT).build();

    inline static const auto FALCON_EVICTION =
        PropertyKey::Builder("main", "falcon_eviction", FALCON, FALCON_DOUBLE).build();

    inline static const auto FALCON_IS_INFERENCE =
        PropertyKey::Builder("main", "falcon_is_inference", FALCON, FALCON_BOOL).build();

    inline static const auto FALCON_MOUNT_PATH =
        PropertyKey::Builder("main", "falcon_mount_path", FALCON, FALCON_STRING).build();

    inline static const auto FALCON_TO_LOCAL =
        PropertyKey::Builder("main", "falcon_to_local", FALCON, FALCON_BOOL).build();

    inline static const auto FALCON_LOG_RESERVED_NUM =
        PropertyKey::Builder("main", "falcon_log_reserved_num", FALCON, FALCON_UINT).build();

    inline static const auto FALCON_LOG_RESERVED_TIME =
        PropertyKey::Builder("main", "falcon_log_reserved_time", FALCON, FALCON_UINT).build();

    inline static const auto FALCON_STAT_MAX = 
        PropertyKey::Builder("main", "falcon_stat_max", FALCON, FALCON_BOOL).build();

    inline static const auto FALCON_USE_PROMETHEUS = 
        PropertyKey::Builder("main", "falcon_use_prometheus", FALCON, FALCON_BOOL).build();

    inline static const auto FALCON_PROMETHEUS_PORT =
        PropertyKey::Builder("main", "falcon_prometheus_port", FALCON, FALCON_STRING).build();
};
