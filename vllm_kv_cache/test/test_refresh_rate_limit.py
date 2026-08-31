#!/usr/bin/env python3
import os
import socket
import sys
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

try:
    from falconfs_kv import falconfs_kv_brpc  # noqa: E402
    _HAS_EXT = True
except Exception:
    _HAS_EXT = False


def _can_reach(host: str, port: int, timeout_s: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except OSError:
        return False


@unittest.skipUnless(_HAS_EXT and _can_reach("127.0.0.1", 55500), "CN/extension not reachable")
class RefreshRateLimitTest(unittest.TestCase):
    def test_membership_refresh_rate_limit(self):
        conninfo = os.environ.get(
            "FALCON_KV_CN_CONNINFO",
            f"host=127.0.0.1 port=55500 dbname=postgres user={os.environ.get('USER', 'postgres')}",
        )
        falconfs_kv_brpc.membership_start(conninfo)
        try:
            first = falconfs_kv_brpc.membership_refresh(1000)
            second = falconfs_kv_brpc.membership_refresh(1000)
            self.assertEqual(first[2], second[2], "refresh should be rate-limited")
            time.sleep(2.1)
            third = falconfs_kv_brpc.membership_refresh(1000)
            self.assertGreaterEqual(third[2], second[2] + 1)
        finally:
            falconfs_kv_brpc.membership_stop()


if __name__ == "__main__":
    unittest.main()
