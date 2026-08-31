#include <gtest/gtest.h>

#include "vllm_kv_cache/src/store/kv_store_facade.h"

namespace falconfs::kv {

TEST(MembershipRefreshRateLimit, GenerationMonotonicAcrossRefreshNow) {
    KVStoreFacadeRegistry reg;
    reg.Start("nohost", "");
    const uint64_t a = reg.Generation();
    (void)reg.RefreshNow(1);
    const uint64_t b = reg.Generation();
    (void)reg.RefreshNow(1);
    const uint64_t c = reg.Generation();
    EXPECT_GE(b, a);
    EXPECT_GE(c, b);
    reg.Stop();
}

}  // namespace falconfs::kv
