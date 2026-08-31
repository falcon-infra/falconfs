#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv.reference import (  # noqa: E402
    BlockStatus,
    ErrorCode,
    MetadataService,
    StoreRegion,
    StoreRegionRegistry,
)


class MetadataReferenceTest(unittest.TestCase):
    def setUp(self):
        self.registry = StoreRegionRegistry()
        self.registry.register(
            StoreRegion(
                store_node_id=0,
                owner_dn_id=1,
                base_offset=4096,
                region_bytes=4 * 1024,
                block_size=1024,
                store_epoch=11,
            )
        )
        self.meta = MetadataService(self.registry, dn_id=1, dn_epoch=7)

    def test_allocate_writes_allocated_row_with_version_and_lease(self):
        result, row, lease = self.meta.allocate(["a"])["a"]
        self.assertTrue(result.success)
        self.assertEqual(row.status, BlockStatus.ALLOCATED)
        self.assertEqual(row.version, 1)
        self.assertEqual(row.location.pool_offset, 4096)
        self.assertEqual(lease.dn_epoch, 7)
        self.assertEqual(lease.store_epoch, 11)

    def test_byte_key_lookup_semantics_represented_by_exact_hash(self):
        self.meta.allocate(["abc"])
        self.assertTrue(self.meta.lookup(["abc"])["abc"][0].success is False)
        self.meta.update_status("abc", BlockStatus.ALLOCATED, BlockStatus.STORED, 1)
        self.assertTrue(self.meta.lookup(["abc"])["abc"][0].success)
        self.assertFalse(self.meta.lookup(["ABC"])["ABC"][0].success)

    def test_cas_status_update_success_and_conflict(self):
        self.meta.allocate(["a"])
        result, row = self.meta.update_status("a", BlockStatus.ALLOCATED, BlockStatus.STORED, 1)
        self.assertTrue(result.success)
        self.assertEqual(row.version, 2)

        conflict, current = self.meta.update_status("a", BlockStatus.ALLOCATED, BlockStatus.STORED, 1)
        self.assertFalse(conflict.success)
        self.assertEqual(conflict.error_code, ErrorCode.CAS_CONFLICT)
        self.assertTrue(conflict.retryable)
        self.assertEqual(current.status, BlockStatus.STORED)

    def test_tuple_concurrent_update_maps_to_cas_conflict_model(self):
        self.meta.allocate(["a"])
        first, _ = self.meta.update_status("a", BlockStatus.ALLOCATED, BlockStatus.STORED, 1)
        second, _ = self.meta.update_status("a", BlockStatus.ALLOCATED, BlockStatus.FAILED, 1)
        self.assertTrue(first.success)
        self.assertEqual(second.error_code, ErrorCode.CAS_CONFLICT)
        self.assertTrue(second.retryable)

    def test_recovery_scan_rebuilds_bitmap_lru_and_lease(self):
        self.meta.allocate(["a", "b"])
        self.meta.update_status("a", BlockStatus.ALLOCATED, BlockStatus.STORED, 1)
        self.meta.update_status("b", BlockStatus.ALLOCATED, BlockStatus.EVICTING, 1)

        rebuilt = MetadataService(self.registry, dn_id=1, dn_epoch=8)
        rebuilt.rows = self.meta.rows.copy()
        for row in rebuilt.rows.values():
            if row.status == BlockStatus.STORED:
                rebuilt.lru.add_stored(row.block_hash)
                rebuilt.lease_manager.grant(
                    row.block_hash, row.location.store_epoch, now_ms=row.updated_at_ms + 1
                )

        self.assertEqual(rebuilt.lru.cold_candidates(10), ["a"])
        lookup = rebuilt.lookup(["a"], renew_lease_on_hit=True)
        self.assertTrue(lookup["a"][0].success)
        self.assertEqual(lookup["a"][2].dn_epoch, 8)


if __name__ == "__main__":
    unittest.main()
