#include <brpc/channel.h>
#include <google/protobuf/stubs/common.h>
#include <cstdint>
#include <iostream>
#include <string>

#include "kv_common.pb.h"
#include "kv_metadata_service.pb.h"
#include "tests/falcon_kv/kv_e2e_block_size.h"

namespace {

int Fail(const std::string &msg)
{
    std::cerr << "E2E_FAIL: " << msg << std::endl;
    return 1;
}

} // namespace

int main(int argc, char **argv)
{
    std::string endpoint = "127.0.0.1:55530";
    std::string block_hash = "kv-meta-e2e-default";
    for (int i = 1; i < argc; ++i) {
        const std::string arg(argv[i]);
        if (arg == "--endpoint" && i + 1 < argc) {
            endpoint = argv[++i];
        } else if (arg == "--block-hash" && i + 1 < argc) {
            block_hash = argv[++i];
        }
    }

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 5000;
    options.connect_timeout_ms = 5000;
    options.max_retry = 1;
    if (channel.Init(endpoint.c_str(), &options) != 0) {
        return Fail("failed to init brpc channel to " + endpoint);
    }
    falconfs::kv::KVMetadataService_Stub stub(&channel);

    // 1) Lookup miss before allocate.
    falconfs::kv::BatchLookupRequest lookup_req0;
    lookup_req0.mutable_meta()->set_client_id(901);
    lookup_req0.mutable_meta()->set_request_id("e2e_lookup_miss_" + block_hash);
    auto *lookup_item0 = lookup_req0.add_items();
    lookup_item0->set_block_hash(block_hash);
    lookup_item0->set_renew_lease_on_hit(false);
    falconfs::kv::BatchLookupResponse lookup_rsp0;
    brpc::Controller cntl_lookup0;
    stub.BatchLookupWithLease(&cntl_lookup0, &lookup_req0, &lookup_rsp0, nullptr);
    if (cntl_lookup0.Failed()) {
        return Fail("lookup(miss) rpc failed: " + cntl_lookup0.ErrorText());
    }
    if (lookup_rsp0.results_size() != 1) {
        return Fail("lookup(miss) response size mismatch");
    }
    if (lookup_rsp0.results(0).result().success()) {
        return Fail("lookup(miss) unexpectedly succeeded");
    }

    // 2) Allocate.
    falconfs::kv::BatchAllocateRequest alloc_req;
    alloc_req.mutable_meta()->set_client_id(901);
    alloc_req.mutable_meta()->set_request_id("e2e_alloc_" + block_hash);
    alloc_req.set_deduplicate_in_request(true);
    auto *alloc_item = alloc_req.add_items();
    alloc_item->set_block_hash(block_hash);
    alloc_item->set_block_size(falconfs::kv::test::E2eKvBlockSize());
    alloc_item->set_preferred_store_id(1);
    alloc_item->set_allow_fallback_store(true);

    falconfs::kv::BatchAllocateResponse alloc_rsp;
    brpc::Controller cntl_alloc;
    stub.BatchAllocateWithLease(&cntl_alloc, &alloc_req, &alloc_rsp, nullptr);
    if (cntl_alloc.Failed()) {
        return Fail("allocate rpc failed: " + cntl_alloc.ErrorText());
    }
    if (alloc_rsp.results_size() != 1 || !alloc_rsp.results(0).result().success()) {
        return Fail("allocate failed");
    }
    const auto &alloc_result = alloc_rsp.results(0);
    if (!alloc_result.has_location() || !alloc_result.has_lease()) {
        return Fail("allocate response missing location/lease");
    }

    // 3) ALLOCATED -> STORED.
    falconfs::kv::BatchUpdateStatusRequest update_req;
    update_req.mutable_meta()->set_client_id(901);
    update_req.mutable_meta()->set_request_id("e2e_update_" + block_hash);
    auto *update_item = update_req.add_items();
    update_item->set_block_hash(block_hash);
    update_item->set_expected_from_status(falconfs::kv::BLOCK_STATUS_ALLOCATED);
    update_item->set_to_status(falconfs::kv::BLOCK_STATUS_STORED);
    update_item->set_expected_version(alloc_result.version());
    update_item->set_allow_noop_if_already_target(true);

    falconfs::kv::BatchUpdateStatusResponse update_rsp;
    brpc::Controller cntl_update;
    stub.BatchUpdateBlockStatus(&cntl_update, &update_req, &update_rsp, nullptr);
    if (cntl_update.Failed()) {
        return Fail("update status rpc failed: " + cntl_update.ErrorText());
    }
    if (update_rsp.results_size() != 1 || !update_rsp.results(0).result().success()) {
        std::string details;
        if (update_rsp.results_size() > 0) {
            const auto &r = update_rsp.results(0);
            details = " success=" + std::to_string(r.result().success()) +
                      " error_code=" + std::to_string(r.result().error_code()) +
                      " retryable=" + std::to_string(r.result().retryable()) +
                      " msg=" + r.result().error_message() +
                      " current_status=" + std::to_string(r.current_status()) +
                      " new_version=" + std::to_string(r.new_version());
        }
        return Fail("update status failed:" + details);
    }
    const int64_t stored_version = update_rsp.results(0).new_version();

    // 4) Lookup hit + lease renewal.
    falconfs::kv::BatchLookupRequest lookup_req1;
    lookup_req1.mutable_meta()->set_client_id(901);
    lookup_req1.mutable_meta()->set_request_id("e2e_lookup_hit_" + block_hash);
    auto *lookup_item1 = lookup_req1.add_items();
    lookup_item1->set_block_hash(block_hash);
    lookup_item1->set_renew_lease_on_hit(true);
    falconfs::kv::BatchLookupResponse lookup_rsp1;
    brpc::Controller cntl_lookup1;
    stub.BatchLookupWithLease(&cntl_lookup1, &lookup_req1, &lookup_rsp1, nullptr);
    if (cntl_lookup1.Failed()) {
        return Fail("lookup(hit) rpc failed: " + cntl_lookup1.ErrorText());
    }
    if (lookup_rsp1.results_size() != 1) {
        return Fail("lookup(hit) response size mismatch");
    }
    const auto &lookup_result = lookup_rsp1.results(0);
    if (!lookup_result.result().success() || !lookup_result.cacheable() || !lookup_result.has_lease()) {
        return Fail("lookup(hit) did not return cacheable lease");
    }

    // 5) Renew lease.
    falconfs::kv::BatchRenewLeaseRequest renew_req;
    renew_req.mutable_meta()->set_client_id(901);
    renew_req.mutable_meta()->set_request_id("e2e_renew_" + block_hash);
    renew_req.set_requested_ttl_ms(3000);
    auto *renew_item = renew_req.add_items();
    renew_item->set_block_hash(block_hash);
    renew_item->set_lease_token(lookup_result.lease().lease_token());
    renew_item->set_expected_dn_epoch(lookup_result.lease().dn_epoch());
    renew_item->set_expected_store_epoch(lookup_result.lease().store_epoch());
    falconfs::kv::BatchRenewLeaseResponse renew_rsp;
    brpc::Controller cntl_renew;
    stub.BatchRenewLease(&cntl_renew, &renew_req, &renew_rsp, nullptr);
    if (cntl_renew.Failed()) {
        return Fail("renew lease rpc failed: " + cntl_renew.ErrorText());
    }
    if (renew_rsp.results_size() != 1 || !renew_rsp.results(0).result().success()) {
        return Fail("renew lease failed");
    }

    std::cout << "E2E_OK" << std::endl;
    std::cout << "BLOCK_HASH=" << block_hash << std::endl;
    std::cout << "STORE_NODE_ID=" << alloc_result.location().store_node_id() << std::endl;
    std::cout << "POOL_OFFSET=" << alloc_result.location().pool_offset() << std::endl;
    std::cout << "VERSION=" << stored_version << std::endl;
    std::cout << "DN_EPOCH=" << renew_rsp.results(0).lease().dn_epoch() << std::endl;
    std::cout << "STORE_EPOCH=" << renew_rsp.results(0).lease().store_epoch() << std::endl;
    return 0;
}
