from __future__ import annotations

import os
import unittest
from types import SimpleNamespace

from falconfs_kv.offloading_manager import FalconFSOffloadingManager, KVBlockLocation, LoadStoreSpec, STATUS_EVICTED


class _FakeStore:
    def batch_read_payload_views(self, store_id, pool_offsets, store_epochs, block_hashes, block_size):
        del store_id, store_epochs, block_size
        payloads = [memoryview((b"x" * 16) + bytes([i])) for i, _ in enumerate(pool_offsets)]
        return [True] * len(pool_offsets), payloads, ["remote_attachment_buffer"] * len(pool_offsets)


class _FakeSSDStore:
    def read_from_ssd(self, store_id, evicted_path, store_epoch, expected_version=0, block_size=0):
        del store_id, evicted_path, store_epoch, expected_version
        return SimpleNamespace(success=True, error_code=0), b"s" * int(block_size or 16)


def _consume_load_store_spec_without_materializing(spec: LoadStoreSpec) -> int:
    total = 0
    for value in spec.data.values():
        view = memoryview(value)
        total += len(view)
    return total


class ZeroCopyReadPolicyTest(unittest.TestCase):
    def setUp(self):
        self._old = dict(os.environ)
        os.environ["FALCON_KV_CLIENT_ZERO_COPY_READS"] = "1"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._old)

    def test_dram_group_uses_readonly_buffer_objects_when_enabled(self):
        mgr = FalconFSOffloadingManager(
            client_id=1,
            client_hostname="ut",
            shard_table={1: "unused"},
            clusters={1: SimpleNamespace(store=_FakeStore())},
            mode="cluster",
            block_size=16,
        )
        try:
            keys = ["zk0", "zk1"]
            for i, key in enumerate(keys):
                mgr.local_cache[key] = KVBlockLocation(
                    block_hash=key,
                    status=2,
                    store_id=1,
                    pool_offset=i * 16,
                    lease_expire_ms=999999,
                    store_epoch=1,
                    dn_id=1,
                )
            out = mgr._load_dram_key_group(1, 1, keys)
            self.assertEqual(set(out), set(keys))
            self.assertTrue(all(isinstance(v, memoryview) for v in out.values()))
            self.assertTrue(all(v.readonly for v in out.values()))
            snap = mgr.perf_breakdown()["zero_copy_read"]
            self.assertEqual(snap["zero_copy_blocks"], 2)
            self.assertEqual(snap["remote_attachment_views"], 2)
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)

    def test_load_store_spec_consumer_accepts_memoryview_without_bytes(self):
        data = bytearray(b"abcd")
        spec = LoadStoreSpec(data={"k": memoryview(data)})
        self.assertEqual(_consume_load_store_spec_without_materializing(spec), 4)
        self.assertIsInstance(spec.data["k"], memoryview)

    def test_ssd_read_records_zero_copy_materialization_fallback(self):
        mgr = FalconFSOffloadingManager(
            client_id=1,
            client_hostname="ut",
            shard_table={1: "unused"},
            clusters={1: SimpleNamespace(store=_FakeSSDStore())},
            mode="cluster",
            block_size=16,
        )
        try:
            mgr.local_cache["zk-ssd"] = KVBlockLocation(
                block_hash="zk-ssd",
                status=STATUS_EVICTED,
                store_id=1,
                pool_offset=0,
                lease_expire_ms=999999,
                evicted_path="/ssd/zk-ssd",
                store_epoch=1,
                dn_id=1,
            )
            payload = mgr._load_one_key_bytes("zk-ssd")
            self.assertEqual(payload, b"s" * 16)
            snap = mgr.perf_breakdown()["zero_copy_read"]
            self.assertEqual(snap["bytes_materialized_blocks"], 1)
            self.assertEqual(snap["fallback_reasons"].get("ssd_read"), 1)
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)

    def test_preallocated_load_buffers_copy_views_and_return_on_complete_load(self):
        os.environ["FALCON_KV_CLIENT_ZERO_COPY_READS"] = "0"
        os.environ["FALCON_KV_CLIENT_PREALLOC_LOAD_BUFFERS"] = "1"
        mgr = FalconFSOffloadingManager(
            client_id=1,
            client_hostname="ut",
            shard_table={1: "unused"},
            clusters={1: SimpleNamespace(store=_FakeStore())},
            mode="cluster",
            block_size=17,
        )
        try:
            keys = ["pk0", "pk1"]
            for i, key in enumerate(keys):
                mgr.local_cache[key] = KVBlockLocation(
                    block_hash=key,
                    status=2,
                    store_id=1,
                    pool_offset=i * 17,
                    lease_expire_ms=999999,
                    store_epoch=1,
                    dn_id=1,
                )
            out = mgr._load_dram_key_group(1, 1, keys)
            self.assertEqual(set(out), set(keys))
            self.assertTrue(all(isinstance(v, memoryview) for v in out.values()))
            snap = mgr.perf_breakdown()["prealloc_load_buffers"]
            self.assertTrue(snap["enabled"])
            self.assertEqual(snap["blocks"], 2)
            self.assertEqual(snap["outstanding"], 2)
            self.assertEqual(mgr.perf_breakdown()["zero_copy_read"]["zero_copy_blocks"], 0)
            mgr.complete_load(keys)
            snap2 = mgr.perf_breakdown()["prealloc_load_buffers"]
            self.assertEqual(snap2["outstanding"], 0)
            self.assertEqual(snap2["pool_size"], 2)
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    unittest.main()
