#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path
import os
import time
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv.offloading_manager import FalconFSOffloadingManager  # noqa: E402
from falconfs_kv.promote_worker import PromoteWorker  # noqa: E402
from falconfs_kv.reference import (  # noqa: E402
    BlockLocation,
    BlockMeta,
    BlockStatus,
    ItemResult,
)


class _FakeMetadata:
    def __init__(self, row):
        self._row = row
        self.lease_manager = mock.Mock()

    def lookup(self, block_hashes, renew_lease_on_hit=True):
        del renew_lease_on_hit
        out = {}
        for h in block_hashes:
            out[h] = (ItemResult(True), self._row, None)
        return out

    def allocate(self, block_hashes, request_id, client_id, allocate_hint=None):
        del request_id, client_id, allocate_hint
        out = {}
        for h in block_hashes:
            out[h] = (ItemResult(True), self._row, None)
        return out

    def update_status(self, block_hash, expected_from, to_status, expected_version, request_id, client_id):
        del block_hash, expected_from, request_id, client_id
        self._row.status = to_status
        self._row.version = expected_version + 1
        return ItemResult(True), self._row


class _FakeStore:
    def __init__(self):
        self.writes = 0

    def read(self, store_node_id, pool_offset, expected_store_epoch):
        del store_node_id, pool_offset, expected_store_epoch
        return ItemResult(False), b""

    def read_from_ssd(
        self, store_node_id, evicted_path, expected_store_epoch, expected_version=0, block_size=65536
    ):
        del store_node_id, evicted_path, expected_store_epoch, expected_version, block_size
        return ItemResult(True), b"from-ssd"

    def write(self, store_node_id, pool_offset, payload, expected_store_epoch, block_size, expected_version=0, block_hash=b""):
        del store_node_id, pool_offset, payload, expected_store_epoch, block_size, expected_version, block_hash
        self.writes += 1
        return ItemResult(True)


class _FakeCluster:
    def __init__(self, row):
        self.metadata = _FakeMetadata(row)
        self.store = _FakeStore()


class PromoteOnReadTest(unittest.TestCase):
    def test_evicted_load_queues_promote_worker(self):
        row = BlockMeta(
            block_hash="k",
            kv_group_idx=0,
            layer_mask=0,
            status=BlockStatus.EVICTED,
            location=BlockLocation(store_node_id=1, pool_offset=0, evicted_path="/tmp/e", store_epoch=1),
            version=5,
        )
        cluster = _FakeCluster(row)
        manager = FalconFSOffloadingManager(
            client_id=10,
            client_hostname="h",
            shard_table={1: "127.0.0.1:55530"},
            clusters={1: cluster},
            mode="cluster",
        )
        manager._promote_worker = mock.Mock()
        loaded = manager.prepare_load(["k"], req_context=None)
        self.assertEqual(loaded.data["k"], b"from-ssd")
        manager._promote_worker.enqueue.assert_called_once()

    def test_promote_worker_enqueue_is_background(self):
        old_env = {
            k: os.environ.get(k)
            for k in (
                "FALCON_KV_PROMOTE_QUEUE_CAPACITY",
                "FALCON_KV_PROMOTE_MAX_INFLIGHT",
                "FALCON_KV_PROMOTE_MIN_ACCESS_COUNT",
            )
        }
        os.environ["FALCON_KV_PROMOTE_QUEUE_CAPACITY"] = "4"
        os.environ["FALCON_KV_PROMOTE_MAX_INFLIGHT"] = "1"
        os.environ["FALCON_KV_PROMOTE_MIN_ACCESS_COUNT"] = "1"
        try:
            row = BlockMeta(
                block_hash="k-bg",
                kv_group_idx=0,
                layer_mask=0,
                status=BlockStatus.EVICTED,
                location=BlockLocation(store_node_id=1, pool_offset=0, evicted_path="/tmp/e", store_epoch=1),
                version=5,
            )
            cluster = _FakeCluster(row)
            manager = mock.Mock()
            manager.client_id = 99
            manager.block_size = 8
            manager.local_cache = {}
            manager._clusters = {1: cluster}
            worker = PromoteWorker(manager)
            try:
                t0 = time.perf_counter()
                worker.enqueue("k-bg", b"payload", 1, 0, 5, 1, 1)
                self.assertLess(time.perf_counter() - t0, 0.05)
                deadline = time.time() + 2.0
                while time.time() < deadline and cluster.store.writes < 1:
                    time.sleep(0.01)
                self.assertEqual(cluster.store.writes, 1)
                stats = worker.stats()
                self.assertEqual(stats["enqueued"], 1)
                self.assertEqual(stats["promoted"], 1)
            finally:
                worker.close()
        finally:
            for k, v in old_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_promote_worker_drops_on_pressure(self):
        old = os.environ.get("FALCON_KV_PROMOTE_WHEN_PRESSURE_BELOW")
        os.environ["FALCON_KV_PROMOTE_WHEN_PRESSURE_BELOW"] = "0.5"
        try:
            manager = mock.Mock()
            manager.promote_pressure_ratio.return_value = 0.9
            worker = PromoteWorker(manager)
            try:
                worker.enqueue("hot", b"payload", 1, 0, 1, 1, 1)
                stats = worker.stats()
                self.assertEqual(stats["dropped_pressure"], 1)
                self.assertEqual(stats["enqueued"], 0)
            finally:
                worker.close()
        finally:
            if old is None:
                os.environ.pop("FALCON_KV_PROMOTE_WHEN_PRESSURE_BELOW", None)
            else:
                os.environ["FALCON_KV_PROMOTE_WHEN_PRESSURE_BELOW"] = old

    def test_promote_worker_queue_full_metric(self):
        old = {k: os.environ.get(k) for k in ("FALCON_KV_ENABLE_PROMOTE_ON_READ", "FALCON_KV_PROMOTE_QUEUE_CAPACITY", "FALCON_KV_PROMOTE_MAX_INFLIGHT")}
        os.environ["FALCON_KV_ENABLE_PROMOTE_ON_READ"] = "0"
        os.environ["FALCON_KV_PROMOTE_QUEUE_CAPACITY"] = "1"
        os.environ["FALCON_KV_PROMOTE_MAX_INFLIGHT"] = "1"
        try:
            manager = mock.Mock()
            worker = PromoteWorker(manager)
            worker._enabled = True
            worker.enqueue("a", b"payload", 1, 0, 1, 1, 1)
            worker.enqueue("b", b"payload", 1, 0, 1, 1, 1)
            stats = worker.stats()
            self.assertEqual(stats["enqueued"], 1)
            self.assertEqual(stats["dropped_queue_full"], 1)
        finally:
            worker.close()
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v


if __name__ == "__main__":
    unittest.main()
