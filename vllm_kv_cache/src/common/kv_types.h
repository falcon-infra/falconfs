// FalconFS KV Cache common types (native C++ port of the Python reference).
//
// These types intentionally mirror the proto definitions in
// `vllm_kv_cache/proto/kv_common.proto` and the Python reference enums in
// `vllm_kv_cache/python/falconfs_kv/reference.py`. They are header-only and
// usable by both the eventual PG-extension service and standalone gtest
// binaries.
#pragma once

#include <cstdint>
#include <string>

namespace falconfs::kv {

// Mirrors `BlockStatus` enum values in `kv_common.proto`.
enum class BlockStatus : int16_t {
    UNSPECIFIED = 0,
    ALLOCATED = 1,
    STORED = 2,
    EVICTING = 3,
    EVICTED = 4,
    FAILED = 5,
};

// Mirrors `ErrorCode` enum values in `kv_common.proto`.
enum class ErrorCode : int32_t {
    UNSPECIFIED = 0,
    OK = 1,
    NOT_FOUND = 2,
    INVALID_ARGUMENT = 3,
    CAS_CONFLICT = 4,
    LEASE_EXPIRED = 5,
    LEASE_TOKEN_MISMATCH = 6,
    STALE_EPOCH = 7,
    STORE_WRITE_FAILED = 8,
    THROTTLED = 9,
    CHECKSUM_MISMATCH = 10,
    INTERNAL_ERROR = 11,
};

struct ItemResult {
    bool success = true;
    ErrorCode error_code = ErrorCode::OK;
    bool retryable = false;
    std::string error_message;

    static ItemResult Ok() { return ItemResult{true, ErrorCode::OK, false, {}}; }
    static ItemResult Err(ErrorCode code, bool retryable, std::string message) {
        return ItemResult{false, code, retryable, std::move(message)};
    }
};

struct LeaseInfo {
    int64_t lease_token = 0;
    int64_t lease_expire_ms = 0;
    int64_t dn_epoch = 0;
    int64_t store_epoch = 0;
};

struct BlockLocation {
    int32_t store_node_id = 0;
    int64_t pool_offset = 0;
    std::string evicted_path;
    int64_t store_epoch = 0;
};

}  // namespace falconfs::kv
