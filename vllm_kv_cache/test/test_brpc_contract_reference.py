#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv.reference import BlockStatus, ErrorCode, ReferenceCluster  # noqa: E402


class BRPCContractReferenceTest(unittest.TestCase):
    def setUp(self):
        self.cluster = ReferenceCluster(block_size=1024)

    def test_allocate_write_update_lookup_read_cycle(self):
        alloc = self.cluster.metadata.allocate(["hash0"])["hash0"]
        self.assertTrue(alloc[0].success)
        row = alloc[1]
        lease = alloc[2]
        self.assertIsNotNone(row)
        self.assertIsNotNone(lease)

        write = self.cluster.store.write(
            row.location.store_node_id,
            row.location.pool_offset,
            b"payload",
            row.location.store_epoch,
            1024,
        )
        self.assertTrue(write.success)

        update, updated = self.cluster.metadata.update_status(
            "hash0", BlockStatus.ALLOCATED, BlockStatus.STORED, row.version
        )
        self.assertTrue(update.success)
        self.assertEqual(updated.status, BlockStatus.STORED)

        lookup = self.cluster.metadata.lookup(["hash0"], renew_lease_on_hit=True)["hash0"]
        self.assertTrue(lookup[0].success)
        self.assertEqual(lookup[1].location.pool_offset, row.location.pool_offset)

        read, payload = self.cluster.store.read(
            lookup[1].location.store_node_id,
            lookup[1].location.pool_offset,
            lookup[1].location.store_epoch,
        )
        self.assertTrue(read.success)
        self.assertEqual(payload, b"payload")

    def test_partial_store_write_only_updates_successful_key(self):
        alloc = self.cluster.metadata.allocate(["ok", "bad"])
        ok_row = alloc["ok"][1]
        bad_row = alloc["bad"][1]

        ok_write = self.cluster.store.write(
            ok_row.location.store_node_id,
            ok_row.location.pool_offset,
            b"ok",
            ok_row.location.store_epoch,
            1024,
        )
        bad_write = self.cluster.store.write(
            bad_row.location.store_node_id,
            bad_row.location.pool_offset,
            b"x" * 2048,
            bad_row.location.store_epoch,
            1024,
        )
        self.assertTrue(ok_write.success)
        self.assertFalse(bad_write.success)

        self.cluster.metadata.update_status("ok", BlockStatus.ALLOCATED, BlockStatus.STORED, ok_row.version)
        self.assertEqual(self.cluster.metadata.rows["ok"].status, BlockStatus.STORED)
        self.assertEqual(self.cluster.metadata.rows["bad"].status, BlockStatus.ALLOCATED)

    def test_evicting_lookup_is_retryable_conflict(self):
        alloc = self.cluster.metadata.allocate(["k"])["k"]
        row = alloc[1]
        self.cluster.metadata.update_status("k", BlockStatus.ALLOCATED, BlockStatus.STORED, row.version)
        self.cluster.metadata.update_status("k", BlockStatus.STORED, BlockStatus.EVICTING, 2)

        result, _, _ = self.cluster.metadata.lookup(["k"])["k"]
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, ErrorCode.CAS_CONFLICT)
        self.assertTrue(result.retryable)


if __name__ == "__main__":
    unittest.main()
