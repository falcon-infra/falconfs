#!/usr/bin/env python3
"""Two-phase eviction and free-allocated tests against MetadataService.

Covers acceptance items A7 (two-phase eviction success + rollback) and A8
(complete_store cleanup path) at the metadata level.
"""
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


def _service():
    registry = StoreRegionRegistry()
    registry.register(
        StoreRegion(
            store_node_id=0,
            owner_dn_id=1,
            base_offset=0,
            region_bytes=2 * 1024,
            block_size=1024,
            store_epoch=4,
        )
    )
    return MetadataService(registry, dn_id=1, dn_epoch=1)


class TwoPhaseEvictionTest(unittest.TestCase):
    def test_success_path_frees_bitmap_and_records_path(self):
        meta = _service()
        meta.allocate(["k"])
        ok, row = meta.update_status("k", BlockStatus.ALLOCATED, BlockStatus.STORED, 1)
        self.assertTrue(ok.success)
        begin, row = meta.begin_eviction("k", expected_version=row.version)
        self.assertTrue(begin.success)
        self.assertEqual(row.status, BlockStatus.EVICTING)

        commit, row = meta.commit_eviction("k", expected_version=row.version, evicted_path="/tmp/x.kv")
        self.assertTrue(commit.success)
        self.assertEqual(row.status, BlockStatus.EVICTED)
        self.assertEqual(row.location.evicted_path, "/tmp/x.kv")
        region = meta.registry.for_dn(1)[0]
        self.assertEqual(region.free_blocks, region.total_blocks)

    def test_rollback_path_restores_stored_and_lru(self):
        meta = _service()
        meta.allocate(["k"])
        meta.update_status("k", BlockStatus.ALLOCATED, BlockStatus.STORED, 1)
        meta.begin_eviction("k", expected_version=2)
        rollback, row = meta.rollback_eviction("k", expected_version=3)
        self.assertTrue(rollback.success)
        self.assertEqual(row.status, BlockStatus.STORED)
        self.assertIn("k", meta.lru._items)

    def test_commit_eviction_requires_path(self):
        meta = _service()
        meta.allocate(["k"])
        meta.update_status("k", BlockStatus.ALLOCATED, BlockStatus.STORED, 1)
        meta.begin_eviction("k", expected_version=2)
        bad, _ = meta.commit_eviction("k", expected_version=3, evicted_path="")
        self.assertFalse(bad.success)
        self.assertEqual(bad.error_code, ErrorCode.INVALID_ARGUMENT)


class FreeAllocatedTest(unittest.TestCase):
    def test_free_releases_bitmap_and_lease(self):
        meta = _service()
        meta.allocate(["k"])
        ok, new_version = meta.free_allocated("k", expected_version=1)
        self.assertTrue(ok.success)
        self.assertEqual(new_version, 2)
        self.assertNotIn("k", meta.rows)
        region = meta.registry.for_dn(1)[0]
        self.assertEqual(region.free_blocks, region.total_blocks)

    def test_cannot_free_stored_without_force(self):
        meta = _service()
        meta.allocate(["k"])
        meta.update_status("k", BlockStatus.ALLOCATED, BlockStatus.STORED, 1)
        bad, _ = meta.free_allocated("k", expected_version=2)
        self.assertFalse(bad.success)
        self.assertEqual(bad.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_force_free_works_for_any_status(self):
        meta = _service()
        meta.allocate(["k"])
        meta.update_status("k", BlockStatus.ALLOCATED, BlockStatus.STORED, 1)
        ok, _ = meta.free_allocated("k", expected_version=2, force=True)
        self.assertTrue(ok.success)


if __name__ == "__main__":
    unittest.main()
