// Shared KV logical block size for C++ BRPC end-to-end tests against a live
// cluster. Must match ``ResolveDnKvBlockSize()`` in falcon/brpc_comm_adapter/
// falcon_brpc_server.cpp and ``FALCON_KV_STORE_BLOCK_SIZE`` exported by
// scripts/falcon_distributed_test.sh (default 2 MiB vLLM-style slice).
#pragma once

#include <cstdint>
#include <cstdlib>

namespace falconfs::kv::test {

constexpr int32_t kVllmDefaultKvBlockBytes = 2 * 32 * 8 * 128 * 16 * 2;

inline int32_t E2eKvBlockSize() {
    const char* v = std::getenv("FALCON_KV_STORE_BLOCK_SIZE");
    if (v == nullptr || v[0] == '\0') {
        return kVllmDefaultKvBlockBytes;
    }
    char* end = nullptr;
    long x = std::strtol(v, &end, 10);
    if (end == v || x <= 0 || x > 2147483647L) {
        return kVllmDefaultKvBlockBytes;
    }
    return static_cast<int32_t>(x);
}

}  // namespace falconfs::kv::test
