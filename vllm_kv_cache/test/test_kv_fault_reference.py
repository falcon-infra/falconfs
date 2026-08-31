#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv.reference import (  # noqa: E402
    BlockStatus,
    ErrorCode,
    ReferenceCluster,
    StoreRegionState,
)


class KVFaultReferenceTest(unittest.TestCase):
    def test_store_suspect_offline_removes_region_from_allocation(self):
        cluster = ReferenceCluster(block_size=1024)
        region = cluster.registry.for_dn(1)[0]
        region.last_heartbeat_ms = 100

        cluster.registry.refresh_health(3101)
        self.assertEqual(region.state, StoreRegionState.SUSPECT)
        allocation = cluster.metadata.allocate(["suspect"])["suspect"]
        self.assertFalse(allocation[0].success)
        self.assertEqual(allocation[0].error_code, ErrorCode.THROTTLED)

        cluster.registry.refresh_health(10101)
        self.assertEqual(region.state, StoreRegionState.OFFLINE)
        allocation = cluster.metadata.allocate(["offline"])["offline"]
        self.assertFalse(allocation[0].success)
        self.assertEqual(allocation[0].error_code, ErrorCode.THROTTLED)

    def test_store_restart_new_epoch_quarantines_then_fences_old_location(self):
        cluster = ReferenceCluster(block_size=1024)
        alloc = cluster.metadata.allocate(["k"])["k"]
        row = alloc[1]
        cluster.store.write(
            row.location.store_node_id,
            row.location.pool_offset,
            b"payload",
            row.location.store_epoch,
            1024,
        )
        old_epoch = row.location.store_epoch

        cluster.registry.heartbeat(row.location.store_node_id, 1000, store_epoch=old_epoch + 1)
        region = cluster.registry.for_dn(1)[0]
        self.assertEqual(region.state, StoreRegionState.QUARANTINED)

        read, _ = cluster.store.read(row.location.store_node_id, row.location.pool_offset, old_epoch)
        self.assertFalse(read.success)
        self.assertEqual(read.error_code, ErrorCode.STALE_EPOCH)

    def test_evicting_lookup_retries_instead_of_returning_half_location(self):
        cluster = ReferenceCluster(block_size=1024)
        alloc = cluster.metadata.allocate(["k"])["k"]
        row = alloc[1]
        cluster.metadata.update_status("k", BlockStatus.ALLOCATED, BlockStatus.STORED, row.version)
        cluster.metadata.update_status("k", BlockStatus.STORED, BlockStatus.EVICTING, 2)

        result, _, _ = cluster.metadata.lookup(["k"])["k"]
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, ErrorCode.CAS_CONFLICT)
        self.assertTrue(result.retryable)


if __name__ == "__main__":
    unittest.main()
