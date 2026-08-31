#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv.reference import (  # noqa: E402
    ErrorCode,
    KVStore,
    StoreRegion,
    StoreRegionRegistry,
    StoreRegionState,
)


class StoreReferenceTest(unittest.TestCase):
    def setUp(self):
        self.registry = StoreRegionRegistry()
        self.region = StoreRegion(
            store_node_id=0,
            owner_dn_id=1,
            base_offset=8192,
            region_bytes=4 * 1024,
            block_size=1024,
            store_epoch=5,
        )
        self.registry.register(self.region)
        self.store = KVStore(self.registry)

    def test_write_read_inside_registered_region(self):
        result = self.store.write(0, 8192, b"hello", expected_store_epoch=5, block_size=1024)
        self.assertTrue(result.success)
        read, payload = self.store.read(0, 8192, expected_store_epoch=5)
        self.assertTrue(read.success)
        self.assertEqual(payload, b"hello")

    def test_reject_offset_outside_region(self):
        result = self.store.write(0, 0, b"bad", expected_store_epoch=5, block_size=1024)
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_reject_stale_store_epoch(self):
        result = self.store.write(0, 8192, b"bad", expected_store_epoch=4, block_size=1024)
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, ErrorCode.STALE_EPOCH)
        self.assertTrue(result.retryable)

    def test_reject_oversized_payload(self):
        result = self.store.write(0, 8192, b"x" * 2048, expected_store_epoch=5, block_size=1024)
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_reject_unhealthy_region(self):
        self.region.state = StoreRegionState.OFFLINE
        result = self.store.write(0, 8192, b"x", expected_store_epoch=5, block_size=1024)
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, ErrorCode.STORE_WRITE_FAILED)
        self.assertTrue(result.retryable)


if __name__ == "__main__":
    unittest.main()
