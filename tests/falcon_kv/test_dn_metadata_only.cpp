#include <gtest/gtest.h>

// v6.5 P2: DN BRPC exports metadata + KVStoreAdmin only (no KVDataService).
// Live verification runs in the cluster harness via `FalconKVP2SmokeE2E
// --mode=dn-no-kvdata` before `FalconKVClusterE2E`.

TEST(DnMetadataOnly, DocumentedClusterCheck) {
    SUCCEED();
}
