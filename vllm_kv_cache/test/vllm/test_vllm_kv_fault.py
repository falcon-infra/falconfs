#!/usr/bin/env python3
import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv.reference import ErrorCode, ReferenceCluster, StoreRegionState  # noqa: E402


class VLLMKVFaultTest(unittest.TestCase):
    def test_store_offline_becomes_cache_miss_or_retryable_failure(self):
        if importlib.util.find_spec("vllm") is None:
            self.skipTest("vLLM is not installed; Store fault semantics covered standalone")

        cluster = ReferenceCluster(block_size=1024)
        region = cluster.registry.for_dn(1)[0]
        region.state = StoreRegionState.OFFLINE
        result = cluster.metadata.allocate(["k"])["k"][0]
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, ErrorCode.THROTTLED)
        self.assertTrue(result.retryable)


if __name__ == "__main__":
    unittest.main()
