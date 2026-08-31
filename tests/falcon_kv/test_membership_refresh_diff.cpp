#include <gtest/gtest.h>

#include "vllm_kv_cache/src/store/kv_store_facade.h"

namespace falconfs::kv {

TEST(MembershipRefreshDiff, ManualRegisterOverridesCatalogView) {
    KVStoreFacadeRegistry reg;
    reg.Start("nohost", "");
    const uint64_t g0 = reg.Generation();
    reg.RegisterRemote(9, "127.0.0.1:9");
    (void)reg.RefreshNow(1000);
    EXPECT_GT(reg.Generation(), g0);
    auto f = reg.Resolve(9);
    ASSERT_NE(f, nullptr);
    EXPECT_EQ(f->StoreNodeId(), 9);
    reg.Stop();
}

}  // namespace falconfs::kv
