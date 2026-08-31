#!/usr/bin/env python3
import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv import FalconFSOffloadingManager  # noqa: E402


class VLLMKVSmokeTest(unittest.TestCase):
    def test_vllm_import_or_skip_and_manager_smoke(self):
        if importlib.util.find_spec("vllm") is None:
            self.skipTest("vLLM is not installed; standalone KV tests cover pre-vLLM behavior")

        manager = FalconFSOffloadingManager(
            shard_table={0: "127.0.0.1:55520"},
            client_id=1,
            client_hostname="localhost",
        )
        spec = manager.prepare_store(["prompt-block"], None)
        self.assertEqual(len(spec.specs), 1)
        manager.complete_store(["prompt-block"], {"prompt-block": b"kv"}, None)
        self.assertTrue(manager.lookup("prompt-block", None))


if __name__ == "__main__":
    unittest.main()
