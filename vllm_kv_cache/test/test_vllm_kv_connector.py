from __future__ import annotations

import sys
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
for path in (ROOT / "third_party" / "vllm", ROOT / "vllm_kv_cache" / "python"):
    s = str(path)
    if s not in sys.path:
        sys.path.insert(0, s)

try:
    import torch
    from falconfs_kv.offloading_manager import KVBlockLocation, LoadStoreSpec, STATUS_STORED
    from falconfs_kv.vllm_kv_connector import (
        FalconFSBlockLocationSpec,
        FalconFSConnectorScheduler,
        FalconFSKeyNamespace,
        FalconFSLoadStoreSpec,
        FalconFSOffloadingManagerAdapter,
        _FalconFSTransferHandler,
        offload_key_to_falcon_key,
    )
    from vllm.config.kv_transfer import KVTransferConfig
    from vllm.v1.kv_offload.abstract import ReqContext, make_offload_key
    from vllm.v1.kv_offload.mediums import GPULoadStoreSpec
    from vllm.v1.kv_offload.spec import CanonicalKVCaches, CanonicalKVCacheRef, CanonicalKVCacheTensor
except Exception as exc:  # pragma: no cover - optional vendored vLLM dependency
    torch = None
    IMPORT_ERROR = exc
else:
    IMPORT_ERROR = None


@unittest.skipIf(IMPORT_ERROR is not None, f"vLLM connector imports unavailable: {IMPORT_ERROR}")
class FalconFSVllmConnectorTest(unittest.TestCase):
    def _key(self, n: int):
        return make_offload_key(f"hash-{n}".encode(), 0)

    def test_scheduler_prefix_lookup_uses_lookup_many_once(self):
        class Manager:
            def __init__(self):
                self.calls = []

            def lookup_many(self, keys, req_context):
                self.calls.append(list(keys))
                return {key: (idx < 3) for idx, key in enumerate(keys)}

            def lookup(self, key, req_context):
                raise AssertionError("single-key lookup must not be used")

        sched = object.__new__(FalconFSConnectorScheduler)
        sched.manager = Manager()
        keys = [self._key(i) for i in range(5)]
        out = sched._maximal_prefix_lookup(keys, ReqContext(kv_transfer_params={"request_id": "r"}))
        self.assertEqual(out, 3)
        self.assertEqual(len(sched.manager.calls), 1)
        self.assertEqual(sched.manager.calls[0], keys)

    def test_scheduler_defer_semantics_match_vllm_prefix_lookup(self):
        class Manager:
            def lookup_many(self, keys, req_context):
                return {keys[0]: True, keys[1]: None, keys[2]: True}

        sched = object.__new__(FalconFSConnectorScheduler)
        sched.manager = Manager()
        self.assertIsNone(sched._maximal_prefix_lookup([self._key(i) for i in range(3)], ReqContext()))

    def test_adapter_lookup_many_batches_and_caches_by_request(self):
        class Inner:
            def __init__(self):
                self.calls = []
                self.local_cache = {}

            def batch_lookup(self, keys, req_context):
                self.calls.append(list(keys))
                return {key: True for key in keys}

        inner = Inner()
        adapter = FalconFSOffloadingManagerAdapter(inner_manager=inner)
        keys = [self._key(i) for i in range(4)]
        ctx = ReqContext(kv_transfer_params={"request_id": "req-a"})
        self.assertEqual(adapter.lookup_many(keys, ctx), {key: True for key in keys})
        self.assertEqual(adapter.lookup_many(keys, ctx), {key: True for key in keys})
        self.assertEqual(len(inner.calls), 1)
        self.assertEqual(inner.calls[0], adapter._to_falcon_keys(keys))
        stats = adapter.stats_snapshot()
        self.assertEqual(stats["lookup_many_calls"], 2)
        self.assertEqual(stats["lookup_batches"], 1)
        self.assertEqual(stats["lookup_cache_hits"], 4)

    def test_prepare_load_returns_ordered_falconfs_spec(self):
        class Inner:
            def __init__(self):
                self.local_cache = {}

            def _batch_lookup_impl(self, keys, req_context, renew_lease_on_hit):
                for i, key in enumerate(keys):
                    self.local_cache[key] = KVBlockLocation(
                        block_hash=key,
                        status=STATUS_STORED,
                        store_id=10 + i,
                        pool_offset=1000 + i,
                        lease_expire_ms=999,
                        store_epoch=7,
                        version=3,
                        dn_id=2,
                    )
                return {key: True for key in keys}

        adapter = FalconFSOffloadingManagerAdapter(inner_manager=Inner())
        keys = [self._key(1), self._key(2)]
        spec = adapter.prepare_load(keys, ReqContext())
        self.assertIsInstance(spec, FalconFSLoadStoreSpec)
        self.assertEqual(spec.keys, keys)
        self.assertEqual([loc.store_id for loc in spec.locations], [10, 11])
        self.assertEqual([loc.pool_offset for loc in spec.locations], [1000, 1001])

    def test_prepare_store_and_complete_store_use_metadata_only_completion(self):
        class Inner:
            def __init__(self):
                self.local_cache = {}
                self.marked = []
                self.freed = []

            def _batch_prepare_store_impl(self, keys, req_context):
                self.local_cache[keys[0]] = KVBlockLocation(
                    block_hash=keys[0],
                    status=1,
                    store_id=1,
                    pool_offset=64,
                    lease_expire_ms=1,
                    store_epoch=5,
                    dn_id=9,
                )
                return LoadStoreSpec(specs=[{"block_hash": keys[0], "store_id": 1, "pool_offset": 64, "dn_id": 9}])

            def _batch_mark_stored(self, keys, batch_id):
                self.marked.append((list(keys), batch_id))
                return {key: True for key in keys}

            def _free_allocated_for_keys(self, keys):
                self.freed.extend(keys)

        inner = Inner()
        adapter = FalconFSOffloadingManagerAdapter(inner_manager=inner)
        keys = [self._key(1), self._key(2)]
        out = adapter.prepare_store(keys, ReqContext())
        self.assertEqual(out.keys_to_store, [keys[0]])
        self.assertEqual(out.store_spec.locations[0].store_id, 1)
        adapter.complete_store(out.keys_to_store, success=True)
        self.assertEqual(inner.marked[0][0], [adapter._to_falcon_key(keys[0])])
        self.assertEqual(inner.freed, [])

    def test_transfer_handlers_copy_cpu_tensor_payloads_to_and_from_falconfs(self):
        tensor = torch.arange(32, dtype=torch.int8).reshape(4, 8)
        caches = CanonicalKVCaches(
            tensors=[CanonicalKVCacheTensor(tensor=tensor, page_size_bytes=8)],
            group_data_refs=[[CanonicalKVCacheRef(tensor_idx=0, page_size_bytes=8)]],
        )
        key0 = self._key(0)
        key1 = self._key(1)
        locations = [
            FalconFSBlockLocationSpec(key0, offload_key_to_falcon_key(key0), dn_id=1, store_id=1, pool_offset=0, store_epoch=1),
            FalconFSBlockLocationSpec(key1, offload_key_to_falcon_key(key1), dn_id=1, store_id=1, pool_offset=8, store_epoch=1),
        ]
        falcon_spec = FalconFSLoadStoreSpec([key0, key1], locations)
        gpu_spec = GPULoadStoreSpec([1, 2], group_sizes=(2,))

        class Adapter:
            def __init__(self):
                self.written = None

            def write_prepared_payloads(self, spec, payloads):
                self.written = (spec, payloads)
                return True

            def read_prepared_payloads(self, spec):
                return {
                    key0: bytes([100 + i for i in range(8)]),
                    key1: bytes([110 + i for i in range(8)]),
                }

        adapter = Adapter()
        store = _FalconFSTransferHandler(adapter, caches, block_size_factor=1, gpu_to_falconfs=True)
        self.assertTrue(store.transfer_async(1, (gpu_spec, falcon_spec)))
        result = store.get_finished()
        if not result:
            store.wait({1})
            result = store.get_finished()
        self.assertTrue(result[0].success)
        self.assertEqual(bytes(adapter.written[1][key0]), bytes(range(8, 16)))
        self.assertEqual(bytes(adapter.written[1][key1]), bytes(range(16, 24)))
        self.assertNotIsInstance(adapter.written[1][key0], bytes)

        load = _FalconFSTransferHandler(adapter, caches, block_size_factor=1, gpu_to_falconfs=False)
        self.assertTrue(load.transfer_async(2, (falcon_spec, gpu_spec)))
        result = load.get_finished()
        if not result:
            load.wait({2})
            result = load.get_finished()
        self.assertTrue(result[0].success)
        self.assertEqual(tensor[1].tolist(), [100 + i for i in range(8)])
        self.assertEqual(tensor[2].tolist(), [110 + i for i in range(8)])
        store.shutdown()
        load.shutdown()


    def _fake_vllm_config(self, *, role="kv_both", extra=None, backend="native", connector=None):
        ktc = KVTransferConfig(
            kv_connector=connector,
            kv_role=role,
            kv_connector_extra_config=dict(extra or {}),
        )
        return SimpleNamespace(
            cache_config=SimpleNamespace(
                kv_offloading_size=1.0,
                kv_offloading_backend=backend,
                block_size=16,
                prefix_caching_hash_algo="sha256",
            ),
            kv_transfer_config=ktc,
            parallel_config=SimpleNamespace(
                tensor_parallel_size=1,
                pipeline_parallel_size=1,
            ),
            model_config=SimpleNamespace(model="unit-model"),
        )

    def test_dynamic_connector_config_is_supported_without_vllm_patch(self):
        cfg = self._fake_vllm_config(
            extra={
                "cn_conninfo": "dbname=falcon",
                "falconfs_namespace": "ns",
                "falconfs_tenant": "tenant",
            },
            connector="FalconFSConnector",
        )
        cfg.kv_transfer_config.kv_connector_module_path = "falconfs_kv.vllm_kv_connector"
        adapter = FalconFSOffloadingManagerAdapter(
            vllm_config=cfg,
            inner_manager=SimpleNamespace(local_cache={}),
        )
        self.assertEqual(cfg.kv_transfer_config.kv_connector, "FalconFSConnector")
        self.assertEqual(
            cfg.kv_transfer_config.kv_connector_module_path,
            "falconfs_kv.vllm_kv_connector",
        )
        self.assertEqual(adapter.key_namespace.namespace, "ns")
        self.assertEqual(adapter.key_namespace.tenant, "tenant")

    def test_namespaced_keys_are_stable_and_isolated(self):
        key = self._key(44)
        ns_a1 = FalconFSKeyNamespace(namespace="n", tenant="t", model_id="m")
        ns_a2 = FalconFSKeyNamespace(namespace="n", tenant="t", model_id="m")
        ns_b = FalconFSKeyNamespace(namespace="n", tenant="t", model_id="other")
        self.assertEqual(ns_a1.to_falcon_key(key), ns_a2.to_falcon_key(key))
        self.assertNotEqual(ns_a1.to_falcon_key(key), ns_b.to_falcon_key(key))
        self.assertTrue(ns_a1.to_falcon_key(key).startswith("ffkv:v1:n:t:"))

    def test_legacy_unnamespaced_key_mode_is_explicit(self):
        key = self._key(45)
        ns = FalconFSKeyNamespace(legacy_unnamespaced_keys=True)
        self.assertEqual(ns.to_falcon_key(key), offload_key_to_falcon_key(key))

    def test_role_policy_skips_producer_load_and_consumer_store(self):
        class Inner:
            local_cache = {}

            def batch_lookup(self, keys, req_context):
                raise AssertionError("producer must not lookup")

        producer = FalconFSOffloadingManagerAdapter(
            vllm_config=self._fake_vllm_config(role="kv_producer"),
            inner_manager=Inner(),
        )
        keys = [self._key(1)]
        self.assertEqual(producer.lookup_many(keys, ReqContext()), {keys[0]: False})
        self.assertTrue(producer.can_store())
        self.assertFalse(producer.can_load())

        consumer = FalconFSOffloadingManagerAdapter(
            vllm_config=self._fake_vllm_config(role="kv_consumer"),
            inner_manager=Inner(),
        )
        out = consumer.prepare_store(keys, ReqContext())
        self.assertEqual(out.keys_to_store, [])
        self.assertTrue(consumer.can_load())
        self.assertFalse(consumer.can_store())


    def test_load_failure_policy_recompute_returns_empty_spec(self):
        class Inner:
            def __init__(self):
                self.local_cache = {}

            def _batch_lookup_impl(self, keys, req_context, renew_lease_on_hit):
                return {key: False for key in keys}

        adapter = FalconFSOffloadingManagerAdapter(
            vllm_config=self._fake_vllm_config(
                extra={"falconfs_load_failure_policy": "recompute"}
            ),
            inner_manager=Inner(),
        )
        spec = adapter.prepare_load([self._key(7)], ReqContext())
        self.assertEqual(spec.keys, [])
        self.assertEqual(adapter.stats_snapshot()["load_failures"], 1)

    def test_load_failure_policy_fail_raises(self):
        class Inner:
            def __init__(self):
                self.local_cache = {}

            def _batch_lookup_impl(self, keys, req_context, renew_lease_on_hit):
                return {key: False for key in keys}

        adapter = FalconFSOffloadingManagerAdapter(
            vllm_config=self._fake_vllm_config(
                extra={"falconfs_load_failure_policy": "fail"}
            ),
            inner_manager=Inner(),
        )
        with self.assertRaisesRegex(RuntimeError, "not found"):
            adapter.prepare_load([self._key(8)], ReqContext())

    def test_write_prepared_payloads_preserves_buffer_protocol(self):
        class Inner:
            def __init__(self):
                self.local_cache = {}
                self.seen_payload = None

            def _complete_store_write_one_key(self, key, data, batch_id, req_context):
                self.seen_payload = data[key]
                return True

        inner = Inner()
        adapter = FalconFSOffloadingManagerAdapter(inner_manager=inner)
        key = self._key(90)
        falcon_key = adapter._to_falcon_key(key)
        inner.local_cache[falcon_key] = KVBlockLocation(
            block_hash=falcon_key,
            status=STATUS_STORED,
            store_id=1,
            pool_offset=0,
            lease_expire_ms=1,
            store_epoch=1,
            dn_id=1,
        )
        spec = FalconFSLoadStoreSpec(
            [key],
            [FalconFSBlockLocationSpec(key, falcon_key, dn_id=1, store_id=1, pool_offset=0, store_epoch=1)],
        )
        payload = memoryview(bytearray(b"abcdefgh"))
        self.assertTrue(adapter.write_prepared_payloads(spec, {key: payload}))
        self.assertIsInstance(inner.seen_payload, memoryview)
        self.assertEqual(bytes(inner.seen_payload), b"abcdefgh")

    def test_async_and_layerwise_config_defaults_and_overrides(self):
        default_adapter = FalconFSOffloadingManagerAdapter(
            vllm_config=self._fake_vllm_config(connector="FalconFSConnector"),
            inner_manager=SimpleNamespace(local_cache={}),
        )
        self.assertTrue(default_adapter.async_load_enabled)
        self.assertFalse(default_adapter.layerwise_load_enabled)
        self.assertEqual(default_adapter.layer_prefetch_depth, 2)

        adapter = FalconFSOffloadingManagerAdapter(
            vllm_config=self._fake_vllm_config(
                connector="FalconFSConnector",
                extra={
                    "falconfs_async_load": False,
                    "falconfs_layerwise_load": True,
                    "falconfs_layer_prefetch_depth": 3,
                },
            ),
            inner_manager=SimpleNamespace(local_cache={}),
        )
        self.assertFalse(adapter.async_load_enabled)
        self.assertTrue(adapter.layerwise_load_enabled)
        self.assertEqual(adapter.layer_prefetch_depth, 3)

    def _layerwise_handler_fixture(self, *, async_load=True, layerwise=True, read_gate=None):
        key = self._key(500)
        tensor = torch.zeros((3, 16), dtype=torch.int8)
        caches = CanonicalKVCaches(
            tensors=[CanonicalKVCacheTensor(tensor=tensor, page_size_bytes=16)],
            group_data_refs=[[CanonicalKVCacheRef(tensor_idx=0, page_size_bytes=16)]],
        )
        falcon_spec = FalconFSLoadStoreSpec(
            [key],
            [FalconFSBlockLocationSpec(key, offload_key_to_falcon_key(key), dn_id=1, store_id=1, pool_offset=0, store_epoch=1)],
        )
        gpu_spec = GPULoadStoreSpec([1], group_sizes=(1,))

        class Adapter:
            def __init__(self):
                self.async_load_enabled = async_load
                self.layerwise_load_enabled = layerwise
                self.layer_prefetch_depth = 1
                self.load_failure_policy = "recompute"
                self.kv_cache_config = SimpleNamespace(
                    kv_cache_groups=[SimpleNamespace(layer_names=["layer.0", "layer.1"])]
                )
                self.stats = {}
                self.read_calls = 0

            def _bump(self, key, count=1):
                self.stats[key] = self.stats.get(key, 0) + count

            def read_prepared_payloads(self, spec):
                self.read_calls += 1
                if read_gate is not None:
                    read_gate[0].set()
                    if not read_gate[1].wait(timeout=2.0):
                        raise TimeoutError("layer-wise async read was not released")
                return {key: bytes(range(16))}

        adapter = Adapter()
        handler = _FalconFSTransferHandler(adapter, caches, block_size_factor=1, gpu_to_falconfs=False)
        return adapter, handler, tensor, falcon_spec, gpu_spec

    def test_whole_block_sync_load_completes_inline_when_async_disabled(self):
        adapter, handler, tensor, falcon_spec, gpu_spec = self._layerwise_handler_fixture(
            async_load=False,
            layerwise=False,
        )
        try:
            self.assertTrue(handler.transfer_async(10, (falcon_spec, gpu_spec)))
            result = handler.get_finished()
            self.assertEqual(len(result), 1)
            self.assertTrue(result[0].success)
            self.assertEqual(adapter.read_calls, 1)
            self.assertEqual(tensor[1].tolist(), list(range(16)))
            self.assertEqual(adapter.stats.get("whole_block_loads"), 1)
        finally:
            handler.shutdown()

    def test_layerwise_sync_load_fetches_once_and_copies_per_wait(self):
        adapter, handler, tensor, falcon_spec, gpu_spec = self._layerwise_handler_fixture(
            async_load=False,
            layerwise=True,
        )
        try:
            self.assertTrue(handler.transfer_async(11, (falcon_spec, gpu_spec)))
            self.assertEqual(adapter.read_calls, 0)
            handler.wait_for_layer_load("layer.0")
            self.assertEqual(adapter.read_calls, 1)
            self.assertEqual(tensor[1, :8].tolist(), list(range(8)))
            self.assertEqual(tensor[1, 8:].tolist(), [0] * 8)
            handler.wait_for_layer_load("layer.1")
            self.assertEqual(adapter.read_calls, 1)
            self.assertEqual(tensor[1].tolist(), list(range(16)))
            result = handler.get_finished()
            self.assertEqual(len(result), 1)
            self.assertTrue(result[0].success)
            self.assertEqual(adapter.stats.get("layerwise_loads"), 1)
            self.assertEqual(adapter.stats.get("layerwise_sync_waits"), 2)
        finally:
            handler.shutdown()

    def test_layerwise_async_load_starts_fetch_before_layer_wait(self):
        read_started = threading.Event()
        read_unblock = threading.Event()
        adapter, handler, tensor, falcon_spec, gpu_spec = self._layerwise_handler_fixture(
            async_load=True,
            layerwise=True,
            read_gate=(read_started, read_unblock),
        )
        try:
            self.assertTrue(handler.transfer_async(12, (falcon_spec, gpu_spec)))
            self.assertTrue(read_started.wait(timeout=2.0))
            self.assertEqual(adapter.read_calls, 1)
            read_unblock.set()
            handler.wait_for_layer_load("layer.0")
            handler.wait_for_layer_load("layer.1")
            result = handler.get_finished()
            self.assertEqual(len(result), 1)
            self.assertTrue(result[0].success)
            self.assertEqual(tensor[1].tolist(), list(range(16)))
            self.assertEqual(adapter.stats.get("layerwise_async_jobs"), 1)
        finally:
            read_unblock.set()
            handler.shutdown()



if __name__ == "__main__":
    unittest.main()
