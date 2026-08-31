// v6.5 P2 smoke checks: DN must not host KVDataService; store must accept writes
// after metadata allocates on a DN pooler.

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "kv_common.pb.h"
#include "kv_data_service.pb.h"
#include "kv_metadata_service.pb.h"
#include "tests/falcon_kv/kv_e2e_block_size.h"

namespace {

bool InitChannel(const std::string& endpoint, int timeout_ms, brpc::Channel* channel) {
    brpc::ChannelOptions options;
    options.protocol            = "baidu_std";
    options.timeout_ms          = timeout_ms;
    options.connect_timeout_ms  = 5000;
    options.max_retry           = 0;
    options.connection_type     = "pooled";
    return channel->Init(endpoint.c_str(), &options) == 0;
}

int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

int Fail(const std::string& msg) {
    std::cerr << "P2_SMOKE_FAIL: " << msg << std::endl;
    return 1;
}

int RunVerifyDnNoKvData(const std::vector<std::string>& dns) {
    for (const auto& ep : dns) {
        brpc::Channel ch;
        if (!InitChannel(ep, 15000, &ch)) {
            return Fail("dn-no-kvdata: channel init failed for " + ep);
        }
        falconfs::kv::KVDataService_Stub stub(&ch);
        falconfs::kv::BatchWriteBlockRequest req;
        req.mutable_meta()->set_request_id("p2_smoke_dn_kvdata_probe");
        auto* it = req.add_items();
        it->set_block_hash("");
        it->set_pool_offset(0);
        it->set_payload("x");
        it->set_block_size(falconfs::kv::test::E2eKvBlockSize());
        it->set_expected_store_epoch(1);
        it->set_expected_version(0);
        falconfs::kv::BatchWriteBlockResponse resp;
        brpc::Controller cntl;
        stub.BatchWriteBlock(&cntl, &req, &resp, nullptr);
        if (!cntl.Failed()) {
            return Fail("dn-no-kvdata: KVDataService still reachable on " + ep +
                        " (v6.5 P2 expects metadata-only on DN pooler)");
        }
    }
    std::cout << "P2_SMOKE_OK dn-no-kvdata endpoints=" << dns.size() << std::endl;
    return 0;
}

int RunVerifyStoreRoundTrip(const std::string& meta_ep, const std::string& store_ep) {
    brpc::Channel mch;
    if (!InitChannel(meta_ep, 60000, &mch)) {
        return Fail("store-smoke: metadata channel init failed");
    }
    falconfs::kv::KVMetadataService_Stub mstub(&mch);
    const std::string h = "cluster_p2smoke_" + std::to_string(NowNs());
    falconfs::kv::BatchAllocateRequest areq;
    areq.mutable_meta()->set_request_id("p2_smoke_alloc");
    auto* ait = areq.add_items();
    ait->set_block_hash(h);
    ait->set_block_size(falconfs::kv::test::E2eKvBlockSize());
    ait->set_preferred_store_id(1);
    ait->set_allow_fallback_store(true);
    falconfs::kv::BatchAllocateResponse aresp;
    brpc::Controller acntl;
    mstub.BatchAllocateWithLease(&acntl, &areq, &aresp, nullptr);
    if (acntl.Failed() || aresp.results_size() != 1 || !aresp.results(0).result().success()) {
        return Fail("store-smoke: allocate failed on " + meta_ep);
    }
    const auto& loc = aresp.results(0).location();
    const int64_t pool_off = loc.pool_offset();
    const int64_t sep      = loc.store_epoch();

    falconfs::kv::BatchUpdateStatusRequest ureq;
    ureq.mutable_meta()->set_request_id("p2_smoke_upd");
    auto* uit = ureq.add_items();
    uit->set_block_hash(h);
    uit->set_expected_from_status(falconfs::kv::BLOCK_STATUS_ALLOCATED);
    uit->set_to_status(falconfs::kv::BLOCK_STATUS_STORED);
    uit->set_expected_version(1);
    falconfs::kv::BatchUpdateStatusResponse ursp;
    brpc::Controller ucntl;
    mstub.BatchUpdateBlockStatus(&ucntl, &ureq, &ursp, nullptr);
    if (ucntl.Failed() || ursp.results_size() != 1 || !ursp.results(0).result().success()) {
        return Fail("store-smoke: update STORED failed");
    }

    brpc::Channel sch;
    if (!InitChannel(store_ep, 60000, &sch)) {
        return Fail("store-smoke: store channel init failed");
    }
    falconfs::kv::KVDataService_Stub sstub(&sch);
    std::string payload(4096, 'z');
    falconfs::kv::BatchWriteBlockRequest wreq;
    wreq.mutable_meta()->set_request_id("p2_smoke_write");
    auto* wit = wreq.add_items();
    wit->set_block_hash("");
    wit->set_pool_offset(pool_off);
    wit->set_payload(payload);
    wit->set_block_size(falconfs::kv::test::E2eKvBlockSize());
    wit->set_expected_store_epoch(sep);
    wit->set_expected_version(0);
    falconfs::kv::BatchWriteBlockResponse wrsp;
    brpc::Controller wcntl;
    sstub.BatchWriteBlock(&wcntl, &wreq, &wrsp, nullptr);
    if (wcntl.Failed() || wrsp.results_size() != 1 || !wrsp.results(0).result().success()) {
        return Fail("store-smoke: BatchWriteBlock failed on store endpoint");
    }

    falconfs::kv::BatchFreeAllocatedRequest freq;
    freq.mutable_meta()->set_request_id("p2_smoke_free");
    auto* fit = freq.add_items();
    fit->set_block_hash(h);
    fit->set_expected_version(2);
    fit->set_force(true);
    falconfs::kv::BatchFreeAllocatedResponse frsp;
    brpc::Controller fcntl;
    mstub.BatchFreeAllocated(&fcntl, &freq, &frsp, nullptr);
    if (fcntl.Failed() || frsp.results_size() != 1 || !frsp.results(0).result().success()) {
        return Fail("store-smoke: free failed");
    }

    std::cout << "P2_SMOKE_OK store-round-trip meta=" << meta_ep << " store=" << store_ep << std::endl;
    return 0;
}

void PrintUsage() {
    std::cerr << "FalconKVP2SmokeE2E --mode=dn-no-kvdata --dn HOST:PORT [--dn HOST:PORT ...]\n"
              << "FalconKVP2SmokeE2E --mode=store-smoke --meta HOST:PORT --store HOST:PORT\n";
}

}  // namespace

int main(int argc, char** argv) {
    std::string mode;
    std::vector<std::string> dns;
    std::string meta_ep;
    std::string store_ep;
    for (int i = 1; i < argc; ++i) {
        const char* a = argv[i];
        if (std::strncmp(a, "--mode=", 7) == 0) {
            mode = a + 7;
        } else if (std::strcmp(a, "--dn") == 0 && i + 1 < argc) {
            dns.push_back(argv[++i]);
        } else if (std::strcmp(a, "--meta") == 0 && i + 1 < argc) {
            meta_ep = argv[++i];
        } else if (std::strcmp(a, "--store") == 0 && i + 1 < argc) {
            store_ep = argv[++i];
        } else {
            PrintUsage();
            return 2;
        }
    }
    if (mode == "dn-no-kvdata") {
        if (dns.empty()) {
            PrintUsage();
            return 2;
        }
        return RunVerifyDnNoKvData(dns);
    }
    if (mode == "store-smoke") {
        if (meta_ep.empty() || store_ep.empty()) {
            PrintUsage();
            return 2;
        }
        return RunVerifyStoreRoundTrip(meta_ep, store_ep);
    }
    PrintUsage();
    return 2;
}
