#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv import FalconFSOffloadingManager  # noqa: E402


class ReqContext:
    kv_group_idx = 0
    layer_mask = 0xFF


class OffloadingManagerAPITest(unittest.TestCase):
    def setUp(self):
        self.manager = FalconFSOffloadingManager(
            shard_table={0: "127.0.0.1:55520"},
            client_id=1,
            client_hostname="localhost",
        )
        self.ctx = ReqContext()

    def test_public_lookup_prepare_store_complete_store_prepare_load(self):
        key = "block-a"
        self.assertFalse(self.manager.lookup(key, self.ctx))

        spec = self.manager.prepare_store([key], self.ctx)
        self.assertEqual(len(spec.specs), 1)

        self.manager.complete_store([key], {key: b"payload"}, self.ctx)
        self.assertTrue(self.manager.lookup(key, self.ctx))

        load = self.manager.prepare_load([key], self.ctx)
        self.assertEqual(load.data[key], b"payload")
        self.manager.complete_load([key], self.ctx)
        self.manager.touch([key], self.ctx)

    def test_prepare_load_missing_key_raises(self):
        with self.assertRaises(RuntimeError):
            self.manager.prepare_load(["missing"], self.ctx)

    def test_partial_complete_store_updates_only_successful_data(self):
        keys = ["ok", "missing-data"]
        self.manager.prepare_store(keys, self.ctx)
        self.manager.complete_store(keys, {"ok": b"ok"}, self.ctx)

        self.assertTrue(self.manager.lookup("ok", self.ctx))
        self.assertFalse(self.manager.lookup("missing-data", self.ctx))


if __name__ == "__main__":
    unittest.main()
