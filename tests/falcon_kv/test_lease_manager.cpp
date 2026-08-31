#include <gtest/gtest.h>

#include "vllm_kv_cache/src/metadata/lease_manager.h"

namespace falconfs::kv {

TEST(LeaseManager, OwnerlessRenewSucceedsWithCorrectEpochsAndToken) {
    LeaseManager mgr(/*dn_epoch=*/7, /*default_ttl_ms=*/1000);
    auto lease = mgr.Grant("k", /*store_epoch=*/3, /*now_ms=*/100);
    auto renew = mgr.Renew("k", lease.lease_token, 7, 3, 200);
    EXPECT_TRUE(renew.result.success);
    EXPECT_GT(renew.lease.lease_expire_ms, lease.lease_expire_ms);
}

TEST(LeaseManager, StaleEpochsRejectedRetryable) {
    LeaseManager mgr(7, 1000);
    auto lease = mgr.Grant("k", /*store_epoch=*/3, /*now_ms=*/100);

    auto stale_dn = mgr.Renew("k", lease.lease_token, /*expected_dn=*/6, 3, 200);
    EXPECT_FALSE(stale_dn.result.success);
    EXPECT_EQ(stale_dn.result.error_code, ErrorCode::STALE_EPOCH);
    EXPECT_TRUE(stale_dn.result.retryable);

    auto stale_store = mgr.Renew("k", lease.lease_token, 7, /*expected_store=*/2, 200);
    EXPECT_FALSE(stale_store.result.success);
    EXPECT_EQ(stale_store.result.error_code, ErrorCode::STALE_EPOCH);
    EXPECT_TRUE(stale_store.result.retryable);
}

TEST(LeaseManager, TokenMismatchAndExpiry) {
    LeaseManager mgr(/*dn_epoch=*/1, /*default_ttl_ms=*/10);
    auto lease = mgr.Grant("k", /*store_epoch=*/1, /*now_ms=*/100);

    auto wrong_token = mgr.Renew("k", lease.lease_token + 1, 1, 1, 101);
    EXPECT_EQ(wrong_token.result.error_code, ErrorCode::LEASE_TOKEN_MISMATCH);

    auto expired = mgr.Renew("k", lease.lease_token, 1, 1, /*now_ms=*/200);
    EXPECT_EQ(expired.result.error_code, ErrorCode::LEASE_EXPIRED);
    EXPECT_TRUE(mgr.CanEvict("k", 200));
}

TEST(LeaseManager, GrantOnExistingLeaseExtendsExpiry) {
    LeaseManager mgr(1, 1000);
    auto a = mgr.Grant("k", /*store_epoch=*/1, /*now_ms=*/100);
    auto b = mgr.Grant("k", 1, /*now_ms=*/200);
    EXPECT_EQ(a.lease_token, b.lease_token);
    EXPECT_GT(b.lease_expire_ms, a.lease_expire_ms);
}

TEST(LeaseManager, DropRemovesLease) {
    LeaseManager mgr(1, 1000);
    mgr.Grant("k", 1, 100);
    mgr.Drop("k");
    EXPECT_FALSE(mgr.Peek("k").has_value());
    auto renew = mgr.Renew("k", /*token=*/1, 1, 1, 200);
    EXPECT_EQ(renew.result.error_code, ErrorCode::LEASE_EXPIRED);
}

}  // namespace falconfs::kv
