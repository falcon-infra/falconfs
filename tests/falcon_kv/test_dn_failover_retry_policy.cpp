#include <gtest/gtest.h>

#include <cstdlib>

#include "vllm_kv_cache/src/store/kv_store_facade.h"

namespace falconfs::kv {

TEST(DnFailoverRetryPolicy, RpcFailureForcesImmediateRefresh) {
    setenv("FALCON_KV_MEMBERSHIP_REFRESH_MIN_INTERVAL_MS", "60000", 1);
    KVStoreFacadeRegistry reg;
    reg.Start("nohost", "");

    (void)reg.RefreshNow(1000);
    const uint64_t g1 = reg.Generation();
    (void)reg.RefreshNow(1000);
    const uint64_t g2 = reg.Generation();
    EXPECT_EQ(g2, g1);

    reg.NotifyRpcFailure(/*store_or_dn_id=*/1, RpcFailureKind::Unavailable);
    (void)reg.RefreshNow(1000);
    const uint64_t g3 = reg.Generation();
    EXPECT_GT(g3, g2);

    reg.Stop();
    unsetenv("FALCON_KV_MEMBERSHIP_REFRESH_MIN_INTERVAL_MS");
}

}  // namespace falconfs::kv
