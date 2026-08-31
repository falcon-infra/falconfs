import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
sys.path.insert(0, str(ROOT / "test"))

import kv_store_microbench as micro
from falconfs_kv import kv_data_service_pb2 as kvdata
from falconfs_kv import store_client


class MicrobenchModeHelpersTest(unittest.TestCase):
    def test_path_mode_aliases(self):
        self.assertEqual(micro._normalize_path_mode("local_shm_zero_copy_read"), "local-shm-zero-copy-read")
        self.assertEqual(micro._normalize_path_mode("remote_brpc_attachment"), "remote-brpc-attachment")
        self.assertEqual(micro._normalize_path_mode("pure_native_memcpy"), "pure-native-memcpy")

    def test_local_read_and_write_bounds_use_distinct_sections(self):
        self.assertEqual(micro._path_section_name("local-shm-zero-copy-read", {}), "local_shm_upper_bound")
        self.assertEqual(micro._path_section_name("local-shm-prealloc-write", {}), "local_shm_prealloc_write_bound")

    def test_local_mode_rejects_remote_store(self):
        locality = {"selected": {1: False}, "all_local": False, "all_remote": True, "known_count": 1}
        with self.assertRaises(SystemExit):
            micro._enforce_locality("local-shm-zero-copy-read", [1], locality, 0)

    def test_remote_mode_rejects_local_store(self):
        locality = {"selected": {1: True}, "all_local": True, "all_remote": False, "known_count": 1}
        with self.assertRaises(SystemExit):
            micro._enforce_locality("remote-brpc-attachment", [1], locality, 0)


class BufferProtocolWriteTest(unittest.TestCase):
    def test_unary_write_passes_memoryview_to_native_payload_api(self):
        captured = {}
        old = store_client.falconfs_kv_brpc.facade_write_block_payload

        def fake(_store_id, _req, payload):
            captured["type"] = type(payload).__name__
            captured["is_memoryview"] = isinstance(payload, memoryview)
            rsp = kvdata.WriteBlockResponse()
            rsp.result.result.success = True
            return rsp.SerializeToString()

        store_client.falconfs_kv_brpc.facade_write_block_payload = fake
        try:
            st = store_client.BrpcKVStore("127.0.0.1:1", use_facade_registry=True)
            result = st.write(1, 0, memoryview(bytearray(b"abcd")), 1, 4, block_hash=b"h")
            self.assertTrue(result.success)
            self.assertTrue(captured.get("is_memoryview"), captured)
        finally:
            store_client.falconfs_kv_brpc.facade_write_block_payload = old

    def test_batch_write_passes_memoryviews_to_native_payload_api(self):
        captured = {}
        old = store_client.falconfs_kv_brpc.facade_batch_write_block_payloads

        def fake(_store_id, _req, payloads):
            captured["all_memoryview"] = all(isinstance(p, memoryview) for p in payloads)
            rsp = kvdata.BatchWriteBlockResponse()
            for _ in payloads:
                item = rsp.results.add()
                item.result.success = True
            return rsp.SerializeToString()

        store_client.falconfs_kv_brpc.facade_batch_write_block_payloads = fake
        try:
            st = store_client.BrpcKVStore("127.0.0.1:1", use_facade_registry=True)
            blocks = [
                store_client.StoreBlockWrite(0, memoryview(bytearray(b"abcd")), b"h0", 4, 1),
                store_client.StoreBlockWrite(4, memoryview(bytearray(b"efgh")), b"h1", 4, 1),
            ]
            results = st.batch_write_blocks(1, blocks)
            self.assertEqual([r.success for r in results], [True, True])
            self.assertTrue(captured.get("all_memoryview"), captured)
        finally:
            store_client.falconfs_kv_brpc.facade_batch_write_block_payloads = old


class MixedMetricsBoundsTest(unittest.TestCase):
    def setUp(self):
        self._old = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._old)

    def _write_bound(self, path: Path, section: str, path_mode: str, write_mb_s: float, read_mb_s: float) -> None:
        path.write_text(json.dumps({
            "mode": "facade",
            "path_mode": path_mode,
            section: {
                "path_mode": path_mode,
                "write": {"wall_mb_s": write_mb_s},
                "read": {"wall_mb_s": read_mb_s},
                "parallelism": 8,
                "block_bytes": 1048576,
            },
        }), encoding="utf-8")

    def test_mixed_e2e_merges_path_specific_microbench_bounds(self):
        import test_offloading_manager_cluster_mixed_e2e as mixed

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            native = root / "native.json"
            local_read = root / "local_read.json"
            local_write = root / "local_write.json"
            remote = root / "remote.json"
            self._write_bound(native, "native_memcpy_bound", "pure-native-memcpy", 30000.0, 30000.0)
            self._write_bound(local_read, "local_shm_upper_bound", "local-shm-zero-copy-read", 12000.0, 11000.0)
            self._write_bound(local_write, "local_shm_prealloc_write_bound", "local-shm-prealloc-write", 10000.0, 9000.0)
            self._write_bound(remote, "remote_brpc_upper_bound", "remote-brpc-attachment", 1800.0, 1600.0)
            os.environ["FALCON_MIX_MICROBENCH_JSONS"] = os.pathsep.join(str(p) for p in (native, local_read, local_write, remote))
            os.environ.pop("FALCON_MIX_MICROBENCH_JSON", None)
            bounds = mixed._microbench_bounds_snapshot()
            self.assertTrue(bounds["available"])
            self.assertIn("local_shm_upper_bound", bounds)
            self.assertIn("local_shm_prealloc_write_bound", bounds)
            self.assertIn("remote_brpc_upper_bound", bounds)
            event = {
                "store_locality": {"1": True, "2": False},
                "local_remote_io_ratio": {
                    "store_writes": {"local": 8, "remote": 8},
                    "load_reads": {"local": 8, "remote": 8},
                },
                "path_throughput_latency": {
                    "store_write": {"local_shm": {"instrumented_mb_s": 5000.0}, "remote_brpc": {"instrumented_mb_s": 900.0}},
                    "load_read": {"local_shm": {"instrumented_mb_s": 5500.0}, "remote_brpc": {"instrumented_mb_s": 800.0}},
                },
            }
            cmp_obj = mixed._path_upper_bound_comparison(event, bounds)
            self.assertIsNotNone(cmp_obj["local_store_pct_of_local_shm_upper"])
            self.assertIsNotNone(cmp_obj["local_load_pct_of_local_shm_upper"])
            mixed._assert_required_upper_bound_comparison(event, bounds, cmp_obj)

    def test_baseline_gate_fails_only_when_explicit_baseline_regresses(self):
        import test_offloading_manager_cluster_mixed_e2e as mixed

        with tempfile.TemporaryDirectory() as td:
            baseline = Path(td) / "baseline.json"
            baseline.write_text(json.dumps([{
                "throughput_end_to_end": {"mb_s_phase_store": 1000.0, "mb_s_phase_load": 1000.0}
            }]), encoding="utf-8")
            os.environ["FALCON_MIX_BASELINE_JSON"] = str(baseline)
            event = {"throughput_end_to_end": {"mb_s_phase_store": 800.0, "mb_s_phase_load": 950.0}}
            comparison = mixed._baseline_comparison_for_event(event)
            with self.assertRaises(AssertionError):
                mixed._enforce_baseline_gate(event, comparison)
            os.environ["FALCON_MIX_ALLOW_PERF_REGRESSION"] = "1"
            mixed._enforce_baseline_gate(event, comparison)


if __name__ == "__main__":
    unittest.main()
