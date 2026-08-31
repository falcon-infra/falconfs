#include <gtest/gtest.h>

#include <cstdint>

#include "connection_pool/kv_catalog_wire.h"
#include "kv_metadata_service.pb.h"

namespace falconfs::kv {

TEST(KvPromoteFromEvicted, CatalogMethodAndWireSizeAreStable) {
    constexpr uint32_t kCount = 3;
    const uint64_t expect = sizeof(uint32_t) + static_cast<uint64_t>(kCount) * sizeof(KVCatalogPromoteResult);
    EXPECT_EQ(KVCatalogResponseSize(::KV_CATALOG_METHOD_PROMOTE_FROM_EVICTED, kCount), expect);
    EXPECT_EQ(static_cast<int>(::KV_CATALOG_METHOD_PROMOTE_FROM_EVICTED), 8);
}

TEST(KvPromoteFromEvicted, ProtoAllocateHintExposesPromoteMode) {
    AllocateItem item;
    item.set_allocate_hint(AllocateHint::ALLOCATE_HINT_PROMOTE_FROM_EVICTED);
    EXPECT_EQ(item.allocate_hint(), AllocateHint::ALLOCATE_HINT_PROMOTE_FROM_EVICTED);
}

}  // namespace falconfs::kv
