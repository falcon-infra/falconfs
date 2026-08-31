#include <gtest/gtest.h>

#include "kv_metadata_service.pb.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_service_impl.h"

namespace falconfs::kv {

TEST(KVMetadataServiceImpl, LookupStubReturnsPerItemInternalError) {
    KVMetadataServiceImpl svc;

    BatchAllocateRequest alloc_req;
    alloc_req.mutable_meta()->set_request_id("alloc");
    auto* item = alloc_req.add_items();
    item->set_block_hash("k1");
    item->set_block_size(65536);
    BatchAllocateResponse alloc_rsp;
    svc.BatchAllocateWithLease(alloc_req, &alloc_rsp);
    ASSERT_EQ(alloc_rsp.results_size(), 1);
    ASSERT_TRUE(alloc_rsp.results(0).result().success());

    BatchUpdateStatusRequest stored_req;
    auto* s = stored_req.add_items();
    s->set_block_hash("k1");
    s->set_expected_from_status(BlockStatus::BLOCK_STATUS_ALLOCATED);
    s->set_to_status(BlockStatus::BLOCK_STATUS_STORED);
    s->set_expected_version(1);
    BatchUpdateStatusResponse stored_rsp;
    svc.BatchUpdateBlockStatus(stored_req, &stored_rsp);
    ASSERT_EQ(stored_rsp.results_size(), 1);
    ASSERT_TRUE(stored_rsp.results(0).result().success());

    BatchLookupRequest lookup_req;
    lookup_req.mutable_meta()->set_request_id("lookup");
    auto* li = lookup_req.add_items();
    li->set_block_hash("k1");
    li->set_renew_lease_on_hit(true);
    BatchLookupResponse lookup_rsp;
    svc.BatchLookupWithLease(lookup_req, &lookup_rsp);

    ASSERT_EQ(lookup_rsp.results_size(), 1);
    const auto& result = lookup_rsp.results(0);
    EXPECT_TRUE(result.result().success());
    EXPECT_EQ(result.status(), BlockStatus::BLOCK_STATUS_STORED);
    EXPECT_EQ(result.location().store_node_id(), 1);
    EXPECT_EQ(result.version(), 2);
    EXPECT_TRUE(result.cacheable());
    EXPECT_GT(result.lease().lease_token(), 0);
    EXPECT_GT(lookup_rsp.server_time_ms(), 0);
}

TEST(KVMetadataServiceImpl, AllocateStubReturnsPerItemInternalError) {
    KVMetadataServiceImpl svc;
    BatchAllocateRequest req;
    auto* item = req.add_items();
    item->set_block_hash("k-alloc");
    item->set_block_size(65536);

    BatchAllocateResponse rsp;
    svc.BatchAllocateWithLease(req, &rsp);

    ASSERT_EQ(rsp.results_size(), 1);
    EXPECT_TRUE(rsp.results(0).result().success());
    EXPECT_FALSE(rsp.results(0).reused_existing_allocation());

    BatchAllocateResponse replay_rsp;
    svc.BatchAllocateWithLease(req, &replay_rsp);
    ASSERT_EQ(replay_rsp.results_size(), 1);
    EXPECT_TRUE(replay_rsp.results(0).result().success());
    EXPECT_TRUE(replay_rsp.results(0).reused_existing_allocation());
}

TEST(KVMetadataServiceImpl, SameRequestIdDoesNotAliasDifferentBlockHashes) {
    KVMetadataServiceImpl svc;
    BatchAllocateRequest req1;
    req1.mutable_meta()->set_request_id("req-idem-alloc");
    req1.mutable_meta()->set_client_id(123);
    auto* a1 = req1.add_items();
    a1->set_block_hash("k-idem-1");
    a1->set_block_size(65536);

    BatchAllocateResponse rsp1;
    svc.BatchAllocateWithLease(req1, &rsp1);
    ASSERT_EQ(rsp1.results_size(), 1);
    ASSERT_TRUE(rsp1.results(0).result().success());
    EXPECT_EQ(rsp1.results(0).block_hash(), "k-idem-1");

    BatchAllocateRequest req2;
    req2.mutable_meta()->set_request_id("req-idem-alloc");
    req2.mutable_meta()->set_client_id(123);
    auto* a2 = req2.add_items();
    a2->set_block_hash("k-idem-2");
    a2->set_block_size(65536);

    BatchAllocateResponse rsp2;
    svc.BatchAllocateWithLease(req2, &rsp2);
    ASSERT_EQ(rsp2.results_size(), 1);
    ASSERT_TRUE(rsp2.results(0).result().success());
    EXPECT_EQ(rsp2.results(0).block_hash(), "k-idem-2");
    EXPECT_NE(rsp2.results(0).location().pool_offset(), rsp1.results(0).location().pool_offset());
}

TEST(KVMetadataServiceImpl, AllocateDeduplicateInRequestReusesFirstResult) {
    KVMetadataServiceImpl svc;
    BatchAllocateRequest req;
    req.set_deduplicate_in_request(true);
    auto* i1 = req.add_items();
    i1->set_block_hash("k-dedup");
    i1->set_block_size(65536);
    auto* i2 = req.add_items();
    i2->set_block_hash("k-dedup");
    i2->set_block_size(65536);

    BatchAllocateResponse rsp;
    svc.BatchAllocateWithLease(req, &rsp);
    ASSERT_EQ(rsp.results_size(), 2);
    EXPECT_TRUE(rsp.results(0).result().success());
    EXPECT_TRUE(rsp.results(1).result().success());
    EXPECT_FALSE(rsp.results(0).reused_existing_allocation());
    EXPECT_FALSE(rsp.results(1).reused_existing_allocation());
    EXPECT_EQ(rsp.results(0).location().pool_offset(), rsp.results(1).location().pool_offset());
}

TEST(KVMetadataServiceImpl, UpdateAndFreeStubsPreserveExpectedVersionEcho) {
    KVMetadataServiceImpl svc;

    BatchAllocateRequest alloc_req;
    auto* a = alloc_req.add_items();
    a->set_block_hash("k-update");
    a->set_block_size(65536);
    BatchAllocateResponse alloc_rsp;
    svc.BatchAllocateWithLease(alloc_req, &alloc_rsp);
    ASSERT_TRUE(alloc_rsp.results(0).result().success());

    BatchUpdateStatusRequest update_req;
    auto* u = update_req.add_items();
    u->set_block_hash("k-update");
    u->set_expected_from_status(BlockStatus::BLOCK_STATUS_ALLOCATED);
    u->set_to_status(BlockStatus::BLOCK_STATUS_STORED);
    u->set_expected_version(1);
    BatchUpdateStatusResponse update_rsp;
    svc.BatchUpdateBlockStatus(update_req, &update_rsp);
    ASSERT_EQ(update_rsp.results_size(), 1);
    EXPECT_TRUE(update_rsp.results(0).result().success());
    EXPECT_EQ(update_rsp.results(0).new_version(), 2);

    BatchFreeAllocatedRequest free_req;
    auto* f = free_req.add_items();
    f->set_block_hash("k-update");
    f->set_expected_version(2);
    BatchFreeAllocatedResponse free_rsp;
    svc.BatchFreeAllocated(free_req, &free_rsp);
    ASSERT_EQ(free_rsp.results_size(), 1);
    EXPECT_FALSE(free_rsp.results(0).result().success());
    EXPECT_EQ(free_rsp.results(0).result().error_code(), ErrorCode::INVALID_ARGUMENT);

    f->set_force(true);
    BatchFreeAllocatedResponse forced_free_rsp;
    svc.BatchFreeAllocated(free_req, &forced_free_rsp);
    ASSERT_EQ(forced_free_rsp.results_size(), 1);
    EXPECT_TRUE(forced_free_rsp.results(0).result().success());
    EXPECT_EQ(forced_free_rsp.results(0).new_version(), 3);
}

TEST(KVMetadataServiceImpl, MutationsKeyedByBlockHashNotRequestIdReplay) {
    KVMetadataServiceImpl svc;

    BatchAllocateRequest alloc_req;
    auto* a = alloc_req.add_items();
    a->set_block_hash("k-idem-update");
    a->set_block_size(65536);
    BatchAllocateResponse alloc_rsp;
    svc.BatchAllocateWithLease(alloc_req, &alloc_rsp);
    ASSERT_TRUE(alloc_rsp.results(0).result().success());

    BatchUpdateStatusRequest update_req1;
    update_req1.mutable_meta()->set_request_id("req-idem-update");
    update_req1.mutable_meta()->set_client_id(77);
    auto* u1 = update_req1.add_items();
    u1->set_block_hash("k-idem-update");
    u1->set_expected_from_status(BlockStatus::BLOCK_STATUS_ALLOCATED);
    u1->set_to_status(BlockStatus::BLOCK_STATUS_STORED);
    u1->set_expected_version(1);
    BatchUpdateStatusResponse update_rsp1;
    svc.BatchUpdateBlockStatus(update_req1, &update_rsp1);
    ASSERT_TRUE(update_rsp1.results(0).result().success());
    EXPECT_EQ(update_rsp1.results(0).new_version(), 2);

    BatchUpdateStatusRequest update_req2;
    update_req2.mutable_meta()->set_request_id("req-idem-update");
    update_req2.mutable_meta()->set_client_id(77);
    auto* u2 = update_req2.add_items();
    u2->set_block_hash("k-idem-update");
    u2->set_expected_from_status(BlockStatus::BLOCK_STATUS_STORED);
    u2->set_to_status(BlockStatus::BLOCK_STATUS_EVICTING);
    u2->set_expected_version(2);
    BatchUpdateStatusResponse update_rsp2;
    svc.BatchUpdateBlockStatus(update_req2, &update_rsp2);
    ASSERT_EQ(update_rsp2.results_size(), 1);
    EXPECT_TRUE(update_rsp2.results(0).result().success());
    EXPECT_EQ(update_rsp2.results(0).new_version(), 3);

    BatchFreeAllocatedRequest free_req1;
    free_req1.mutable_meta()->set_request_id("req-idem-free");
    free_req1.mutable_meta()->set_client_id(77);
    auto* f1 = free_req1.add_items();
    f1->set_block_hash("k-idem-update");
    f1->set_expected_version(3);
    f1->set_force(true);
    BatchFreeAllocatedResponse free_rsp1;
    svc.BatchFreeAllocated(free_req1, &free_rsp1);
    ASSERT_TRUE(free_rsp1.results(0).result().success());
    EXPECT_EQ(free_rsp1.results(0).new_version(), 4);

    BatchFreeAllocatedRequest free_req2;
    free_req2.mutable_meta()->set_request_id("req-idem-free");
    free_req2.mutable_meta()->set_client_id(77);
    auto* f2 = free_req2.add_items();
    f2->set_block_hash("different-block");
    f2->set_expected_version(999);
    f2->set_force(false);
    BatchFreeAllocatedResponse free_rsp2;
    svc.BatchFreeAllocated(free_req2, &free_rsp2);
    ASSERT_EQ(free_rsp2.results_size(), 1);
    EXPECT_EQ(free_rsp2.results(0).block_hash(), "different-block");
    EXPECT_FALSE(free_rsp2.results(0).result().success());
    EXPECT_EQ(free_rsp2.results(0).result().error_code(), ErrorCode::CAS_CONFLICT);
}

TEST(KVMetadataServiceImpl, RemoteLibpqRefusesNonSplitEntries) {
    auto engine = std::make_shared<KVMetadataEngine>();
    engine->SetCatalogTier(EngineCatalogTier::REMOTE_LIBPQ);
    KVMetadataServiceImpl svc(engine);

    auto assert_refusal = [](const ItemResultMeta& m) {
        EXPECT_FALSE(m.success());
        EXPECT_EQ(m.error_code(), ErrorCode::INTERNAL_ERROR);
        EXPECT_FALSE(m.retryable());
        EXPECT_NE(m.error_message().find("SplitForPoolWorker"), std::string::npos);
    };

    BatchLookupRequest lr;
    lr.add_items()->set_block_hash("k");
    BatchLookupResponse lrs;
    svc.BatchLookupWithLease(lr, &lrs);
    ASSERT_EQ(lrs.results_size(), 1);
    assert_refusal(lrs.results(0).result());

    BatchAllocateRequest ar;
    ar.add_items()->set_block_hash("a");
    ar.mutable_items(0)->set_block_size(65536);
    BatchAllocateResponse ars;
    svc.BatchAllocateWithLease(ar, &ars);
    ASSERT_EQ(ars.results_size(), 1);
    assert_refusal(ars.results(0).result());

    BatchRenewLeaseRequest rr;
    auto* ri = rr.add_items();
    ri->set_block_hash("r");
    ri->set_lease_token(1);
    rr.set_requested_ttl_ms(1000);
    BatchRenewLeaseResponse rrs;
    svc.BatchRenewLease(rr, &rrs);
    ASSERT_EQ(rrs.results_size(), 1);
    assert_refusal(rrs.results(0).result());

    BatchUpdateStatusRequest ur;
    auto* ui = ur.add_items();
    ui->set_block_hash("u");
    ui->set_expected_from_status(BlockStatus::BLOCK_STATUS_ALLOCATED);
    ui->set_to_status(BlockStatus::BLOCK_STATUS_STORED);
    ui->set_expected_version(0);
    BatchUpdateStatusResponse urs;
    svc.BatchUpdateBlockStatus(ur, &urs);
    ASSERT_EQ(urs.results_size(), 1);
    assert_refusal(urs.results(0).result());

    BatchFreeAllocatedRequest fr;
    fr.add_items()->set_block_hash("f");
    BatchFreeAllocatedResponse frs;
    svc.BatchFreeAllocated(fr, &frs);
    ASSERT_EQ(frs.results_size(), 1);
    assert_refusal(frs.results(0).result());
}

TEST(KVMetadataServiceImpl, SplitForPoolWorkerLookupInvokesCatalogLambda) {
    KVMetadataServiceImpl svc;
    BatchLookupRequest req;
    req.mutable_meta()->set_request_id("split-lookup-1");
    auto* li = req.add_items();
    li->set_block_hash("catalog-only");
    li->set_renew_lease_on_hit(false);

    int invocations = 0;
    BatchLookupResponse rsp;
    svc.BatchLookupWithLeaseSplitForPoolWorker(req, &rsp, [&](const std::string& sub_payload) {
        ++invocations;
        BatchLookupRequest sub;
        EXPECT_TRUE(sub.ParseFromString(sub_payload));
        EXPECT_EQ(sub.items_size(), 1);
        EXPECT_EQ(sub.items(0).block_hash(), "catalog-only");

        BatchLookupResponse subrsp;
        auto* r = subrsp.add_results();
        r->set_block_hash(sub.items(0).block_hash());
        r->mutable_result()->set_success(true);
        r->mutable_result()->set_error_code(ErrorCode::OK);
        r->set_status(BlockStatus::BLOCK_STATUS_STORED);
        r->mutable_location()->set_store_node_id(1);
        r->mutable_location()->set_pool_offset(65536);
        r->mutable_location()->set_store_epoch(1);
        r->set_version(7);
        r->set_cacheable(true);
        return subrsp.SerializeAsString();
    });

    EXPECT_EQ(invocations, 1);
    ASSERT_EQ(rsp.results_size(), 1);
    EXPECT_TRUE(rsp.results(0).result().success());
    EXPECT_EQ(rsp.results(0).status(), BlockStatus::BLOCK_STATUS_STORED);
    EXPECT_EQ(rsp.results(0).version(), 7);
    EXPECT_EQ(rsp.results(0).location().pool_offset(), 65536);
}

TEST(KVMetadataServiceImpl, SplitForPoolWorker_AllMethods) {
    KVMetadataServiceImpl svc;
    int catalog_batches = 0;

    const auto run_catalog_sub_batch = [&](const std::string& sub) -> std::string {
        ++catalog_batches;

        BatchAllocateRequest ba;
        if (ba.ParseFromString(sub)) {
            const std::string& rid = ba.meta().request_id();
            if (rid.find("sw-alloc") != std::string::npos) {
                BatchAllocateResponse rsp;
                for (int i = 0; i < ba.items_size(); ++i) {
                    const auto& it = ba.items(i);
                    auto* o = rsp.add_results();
                    o->set_block_hash(it.block_hash());
                    o->mutable_result()->set_success(true);
                    o->mutable_result()->set_error_code(ErrorCode::OK);
                    o->mutable_location()->set_store_node_id(it.worker_reserved_store_node_id());
                    o->mutable_location()->set_pool_offset(it.worker_reserved_pool_offset());
                    o->mutable_location()->set_store_epoch(0);
                    o->set_version(0);
                }
                return rsp.SerializeAsString();
            }
        }

        BatchUpdateStatusRequest bu;
        if (bu.ParseFromString(sub)) {
            const std::string& rid = bu.meta().request_id();
            if (rid.find("sw-upd") != std::string::npos) {
                BatchUpdateStatusResponse rsp;
                for (int i = 0; i < bu.items_size(); ++i) {
                    auto* o = rsp.add_results();
                    o->set_block_hash(bu.items(i).block_hash());
                    o->mutable_result()->set_success(true);
                    o->mutable_result()->set_error_code(ErrorCode::OK);
                    o->set_new_version(1);
                    o->set_current_status(BlockStatus::BLOCK_STATUS_STORED);
                }
                return rsp.SerializeAsString();
            }
        }

        BatchFreeAllocatedRequest bf;
        if (bf.ParseFromString(sub)) {
            const std::string& rid = bf.meta().request_id();
            if (rid.find("sw-free") != std::string::npos) {
                BatchFreeAllocatedResponse rsp;
                for (int i = 0; i < bf.items_size(); ++i) {
                    const auto& it = bf.items(i);
                    auto* o = rsp.add_results();
                    o->set_block_hash(it.block_hash());
                    o->mutable_result()->set_success(true);
                    o->mutable_result()->set_error_code(ErrorCode::OK);
                    o->set_new_version(it.expected_version() + 1);
                }
                return rsp.SerializeAsString();
            }
        }

        BatchLookupRequest bl;
        if (bl.ParseFromString(sub)) {
            const std::string& rid = bl.meta().request_id();
            if (rid.find("sw-lk") != std::string::npos) {
                BatchLookupResponse rsp;
                for (int i = 0; i < bl.items_size(); ++i) {
                    auto* o = rsp.add_results();
                    o->set_block_hash(bl.items(i).block_hash());
                    o->mutable_result()->set_success(true);
                    o->mutable_result()->set_error_code(ErrorCode::OK);
                    o->set_status(BlockStatus::BLOCK_STATUS_STORED);
                    o->mutable_location()->set_store_node_id(1);
                    o->mutable_location()->set_pool_offset(131072);
                    o->mutable_location()->set_store_epoch(1);
                    o->set_version(2);
                    o->set_cacheable(true);
                }
                return rsp.SerializeAsString();
            }
        }

        return {};
    };

    // 1) Allocate (Pass-1 bitmap + catalog INSERT merge).
    {
        BatchAllocateRequest req;
        req.mutable_meta()->set_request_id("sw-alloc-1");
        auto* it = req.add_items();
        it->set_block_hash("sw-all-block");
        it->set_block_size(65536);
        BatchAllocateResponse rsp;
        svc.BatchAllocateWithLeaseSplitForPoolWorker(req, &rsp, run_catalog_sub_batch);
        ASSERT_EQ(rsp.results_size(), 1);
        EXPECT_TRUE(rsp.results(0).result().success());
        EXPECT_EQ(rsp.results(0).location().store_node_id(), 1);
        EXPECT_GE(rsp.results(0).location().pool_offset(), 0);
    }

    // 2) ALLOCATED -> STORED (DRAM pre-check passes; catalog CAS simulated).
    {
        BatchUpdateStatusRequest req;
        req.mutable_meta()->set_request_id("sw-upd-1");
        auto* it = req.add_items();
        it->set_block_hash("sw-all-block");
        it->set_expected_from_status(BlockStatus::BLOCK_STATUS_ALLOCATED);
        it->set_to_status(BlockStatus::BLOCK_STATUS_STORED);
        it->set_expected_version(1);
        BatchUpdateStatusResponse rsp;
        svc.BatchUpdateBlockStatusSplitForPoolWorker(req, &rsp, run_catalog_sub_batch);
        ASSERT_EQ(rsp.results_size(), 1);
        EXPECT_TRUE(rsp.results(0).result().success());
        EXPECT_EQ(rsp.results(0).new_version(), 1);
    }

    // 3) Renew with no DRAM slot — v6.4 §11.3: LEASE_EXPIRED, no catalog round-trip.
    {
        BatchRenewLeaseRequest req;
        req.mutable_meta()->set_request_id("sw-ren-1");
        req.set_requested_ttl_ms(2000);
        auto* it = req.add_items();
        it->set_block_hash("phantom-renew");
        it->set_lease_token(99);
        it->set_expected_dn_epoch(1);
        it->set_expected_store_epoch(1);
        BatchRenewLeaseResponse rsp;
        svc.BatchRenewLeaseSplitForPoolWorker(req, &rsp, run_catalog_sub_batch);
        ASSERT_EQ(rsp.results_size(), 1);
        EXPECT_FALSE(rsp.results(0).result().success());
        EXPECT_EQ(rsp.results(0).result().error_code(), ErrorCode::LEASE_EXPIRED);
        EXPECT_TRUE(rsp.results(0).result().retryable());
    }

    // 4) Free catalog-only (no DRAM slot): catalog DELETE simulated.
    {
        BatchFreeAllocatedRequest req;
        req.mutable_meta()->set_request_id("sw-free-1");
        auto* it = req.add_items();
        it->set_block_hash("orphan-free");
        it->set_expected_version(0);
        it->set_force(true);
        BatchFreeAllocatedResponse rsp;
        svc.BatchFreeAllocatedSplitForPoolWorker(req, &rsp, run_catalog_sub_batch);
        ASSERT_EQ(rsp.results_size(), 1);
        EXPECT_TRUE(rsp.results(0).result().success());
        EXPECT_EQ(rsp.results(0).new_version(), 1);
    }

    // 5) Lookup DRAM miss + catalog merge.
    {
        BatchLookupRequest req;
        req.mutable_meta()->set_request_id("sw-lk-1");
        auto* it = req.add_items();
        it->set_block_hash("orphan-lookup");
        it->set_renew_lease_on_hit(false);
        BatchLookupResponse rsp;
        svc.BatchLookupWithLeaseSplitForPoolWorker(req, &rsp, run_catalog_sub_batch);
        ASSERT_EQ(rsp.results_size(), 1);
        EXPECT_TRUE(rsp.results(0).result().success());
        EXPECT_EQ(rsp.results(0).status(), BlockStatus::BLOCK_STATUS_STORED);
        EXPECT_EQ(rsp.results(0).version(), 2);
    }

    EXPECT_EQ(catalog_batches, 4);
}

}  // namespace falconfs::kv
