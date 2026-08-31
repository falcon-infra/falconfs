from __future__ import annotations

import os
import unittest

from falconfs_kv.offloading_manager import BlockStatus, FalconFSOffloadingManager, KVBlockLocation


class AdaptiveBatchingPolicyTest(unittest.TestCase):
    def setUp(self):
        self._old = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._old)

    def _manager(self):
        return FalconFSOffloadingManager(
            client_id=1,
            client_hostname="ut",
            shard_table={1: "unused"},
            mode="reference",
            block_size=1024 * 1024,
        )

    def test_static_defaults_do_not_adapt_when_flag_off(self):
        os.environ["FALCON_KV_CLIENT_ADAPTIVE_BATCHING"] = "0"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_MAX_BLOCKS"] = "8"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_TARGET_BYTES"] = str(2 * 1024 * 1024)
        mgr = self._manager()
        try:
            self.assertEqual(mgr._effective_batch_max_blocks(is_write=False), 8)
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)

    def test_adaptive_flag_caps_by_target_bytes_and_reports_enabled(self):
        os.environ["FALCON_KV_CLIENT_ADAPTIVE_BATCHING"] = "1"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_MAX_BLOCKS"] = "8"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_TARGET_BYTES"] = str(2 * 1024 * 1024)
        mgr = self._manager()
        try:
            self.assertEqual(mgr._effective_batch_max_blocks(is_write=False), 2)
            self.assertTrue(mgr._adaptive_batching_enabled)
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)

    def test_adaptive_controller_accepts_then_rolls_back_regression(self):
        os.environ["FALCON_KV_CLIENT_ADAPTIVE_BATCHING"] = "1"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_MAX_BLOCKS"] = "8"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_TARGET_BYTES"] = str(8 * 1024 * 1024)
        mgr = self._manager()
        try:
            st = mgr._adaptive_batching_state["local_read"]
            st["selected"] = 1
            st["previous"] = 1
            st["last_good"] = 1
            mgr._adaptive_observe_path("local_read", mb_s=1000.0, p50_ms=1.0, p95_ms=2.0, error_rate=0.0)
            self.assertEqual(mgr._adaptive_batching_state["local_read"]["selected"], 2)
            mgr._adaptive_observe_path("local_read", mb_s=1200.0, p50_ms=1.0, p95_ms=2.0, error_rate=0.0)
            self.assertEqual(mgr._adaptive_batching_state["local_read"]["selected"], 3)
            mgr._adaptive_observe_path("local_read", mb_s=800.0, p50_ms=2.0, p95_ms=4.5, error_rate=0.0)
            self.assertEqual(mgr._adaptive_batching_state["local_read"]["selected"], 2)
            snap = mgr._adaptive_batching_snapshot()
            self.assertGreaterEqual(snap["controller"]["local_read"]["rollback_count"], 1)
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)

    def test_adaptive_controller_rolls_back_on_error_rate(self):
        os.environ["FALCON_KV_CLIENT_ADAPTIVE_BATCHING"] = "1"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_MAX_BLOCKS"] = "8"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_TARGET_BYTES"] = str(8 * 1024 * 1024)
        mgr = self._manager()
        try:
            st = mgr._adaptive_batching_state["remote_read"]
            st["selected"] = 1
            st["previous"] = 1
            st["last_good"] = 1
            mgr._adaptive_observe_path("remote_read", mb_s=1000.0, p50_ms=1.0, p95_ms=2.0, error_rate=0.0)
            self.assertEqual(mgr._adaptive_batching_state["remote_read"]["selected"], 2)
            mgr._adaptive_observe_path("remote_read", mb_s=1100.0, p50_ms=1.0, p95_ms=2.0, error_rate=0.25)
            self.assertEqual(mgr._adaptive_batching_state["remote_read"]["selected"], 1)
            self.assertEqual(mgr._adaptive_batching_state["remote_read"]["last_decision_reason"], "rollback_on_regression")
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)

    def test_adaptive_target_honors_cpu_parallelism_and_max_inflight(self):
        os.environ["FALCON_KV_CLIENT_ADAPTIVE_BATCHING"] = "1"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_MAX_BLOCKS"] = "64"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_TARGET_BYTES"] = str(64 * 1024 * 1024)
        os.environ["FALCON_KV_CLIENT_DATA_PARALLELISM_MAX"] = "4"
        os.environ["FALCON_KV_CLIENT_ADAPTIVE_MAX_INFLIGHT_BYTES"] = str(8 * 1024 * 1024)
        mgr = self._manager()
        try:
            self.assertEqual(mgr._target_bound_for_path("local_read", 64), 2)
            self.assertLessEqual(mgr._adaptive_selected_for_path("local_read", 64), 2)
            snap = mgr._adaptive_batching_snapshot()
            self.assertEqual(snap["data_parallelism"], 4)
            self.assertEqual(snap["max_inflight_bytes"], 8 * 1024 * 1024)
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)


    def test_local_write_batch_override_is_honored_when_global_write_batch_is_one(self):
        os.environ["FALCON_KV_OM_PERF"] = "1"
        os.environ["FALCON_KV_CLIENT_BATCH_WRITE_MAX_BLOCKS"] = "1"
        os.environ["FALCON_KV_CLIENT_BATCH_WRITE_LOCAL_MAX_BLOCKS"] = "4"
        os.environ["FALCON_KV_CLIENT_DATA_PARALLELISM_MAX"] = "4"
        mgr = self._manager()
        try:
            mgr._store_is_local = lambda store_id: True  # type: ignore[method-assign]
            keys = [f"k{i}" for i in range(6)]
            for i, key in enumerate(keys):
                mgr.local_cache[key] = KVBlockLocation(
                    block_hash=key,
                    status=int(BlockStatus.ALLOCATED),
                    store_id=7,
                    pool_offset=i * mgr.block_size,
                    lease_expire_ms=0,
                )
            data = {key: b"x" * 8 for key in keys}
            grouped_calls = []
            unary_calls = []

            def group_write(dn_id, store_id, group_keys, data_map, batch_id):
                grouped_calls.append(tuple(group_keys))
                return {key: True for key in group_keys}

            def unary_write(key, data_map, batch_id, req_context):
                unary_calls.append(key)
                return True

            mgr._complete_store_write_key_group = group_write  # type: ignore[method-assign]
            mgr._complete_store_write_one_key = unary_write  # type: ignore[method-assign]
            mgr._batch_mark_stored = lambda written, batch_id: {key: True for key in written}  # type: ignore[method-assign]
            mgr._om_perf_cs_acc = {}

            out = mgr._cluster_batch_complete_store(keys, data, req_context=None)

            self.assertEqual(set(out), set(keys))
            self.assertEqual(unary_calls, [])
            self.assertEqual([len(call) for call in grouped_calls], [4, 2])
            self.assertEqual(mgr._om_perf_cs_acc["write_batching_enabled"], 1.0)
            self.assertEqual(mgr._om_perf_cs_acc["write_batch_local_max_blocks"], 4.0)
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    unittest.main()
