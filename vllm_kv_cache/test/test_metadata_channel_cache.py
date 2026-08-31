from __future__ import annotations

import os
import unittest

from falconfs_kv import falconfs_kv_brpc
from falconfs_kv import kv_metadata_service_pb2 as _kvmeta


class MetadataChannelCacheTest(unittest.TestCase):
    def _lookup_rpc(self, endpoint: str, timeout_ms: int = 3000) -> None:
        req = _kvmeta.BatchLookupRequest()
        req.meta.request_id = "metadata-channel-cache"
        req.meta.client_id = 0
        falconfs_kv_brpc.batch_lookup_with_lease(endpoint, req.SerializeToString(), timeout_ms)

    def test_reuses_successful_metadata_channel_and_evicts_failed_channel(self):
        endpoint = os.environ.get("FALCON_KV_TEST_DN_ENDPOINT", "127.0.0.1:55530")
        falconfs_kv_brpc.metadata_channel_stats_reset()
        try:
            self._lookup_rpc(endpoint)
            self._lookup_rpc(endpoint)
        except Exception as exc:
            self.skipTest(f"metadata endpoint unavailable: {endpoint}: {exc}")

        stats = dict(falconfs_kv_brpc.metadata_channel_stats())
        self.assertGreaterEqual(int(stats.get("cache_size", 0)), 1)
        self.assertGreaterEqual(int(stats.get("misses", 0)), 1)
        self.assertGreaterEqual(int(stats.get("hits", 0)), 1)
        self.assertLessEqual(int(stats.get("misses", 0)), 1)

        bad_endpoint = os.environ.get("FALCON_KV_TEST_BAD_DN_ENDPOINT", "127.0.0.1:9")
        try:
            self._lookup_rpc(bad_endpoint, timeout_ms=200)
        except Exception:
            pass
        else:
            self.skipTest(f"bad metadata endpoint unexpectedly reachable: {bad_endpoint}")

        after = dict(falconfs_kv_brpc.metadata_channel_stats())
        self.assertGreaterEqual(int(after.get("failures", 0)), 1)
        self.assertGreaterEqual(int(after.get("evictions", 0)), 1)


if __name__ == "__main__":
    unittest.main()
