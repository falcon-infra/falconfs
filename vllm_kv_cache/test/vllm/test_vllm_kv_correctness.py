#!/usr/bin/env python3
import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv import FalconFSOffloadingManager  # noqa: E402


class VLLMKVCorrectnessTest(unittest.TestCase):
    def test_cache_roundtrip_matches_expected_payload(self):
        if importlib.util.find_spec("vllm") is None:
            self.skipTest("vLLM is not installed; payload roundtrip is validated without scheduler")

        manager = FalconFSOffloadingManager(
            shard_table={0: "127.0.0.1:55520"},
            client_id=1,
            client_hostname="localhost",
        )
        key = "deterministic-prefix"
        payload = b"deterministic-kv-payload"
        manager.prepare_store([key], None)
        manager.complete_store([key], {key: payload}, None)
        loaded = manager.prepare_load([key], None)
        self.assertEqual(loaded.data[key], payload)


if __name__ == "__main__":
    unittest.main()
