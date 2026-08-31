#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv.offloading_manager import FalconFSOffloadingManager  # noqa: E402


class MembershipLateJoinTest(unittest.TestCase):
    def test_refresh_membership_picks_up_new_dn(self):
        discovered = [{1: "127.0.0.1:55530"}, {1: "127.0.0.1:55530", 2: "127.0.0.1:55550"}]
        calls = []

        def cluster_factory(dn_id, endpoint):
            calls.append((dn_id, endpoint))
            return object()

        brpc = mock.Mock()
        brpc.membership_refresh.return_value = (2, 1, 1)
        brpc.discover_dn_endpoints.side_effect = discovered
        with mock.patch("falconfs_kv.offloading_manager.falconfs_kv_brpc", new=brpc):
            manager = FalconFSOffloadingManager(
                client_id=1,
                client_hostname="host",
                mode="cluster",
                cn_conninfo="host=127.0.0.1 port=55500 dbname=postgres",
                cluster_factory=cluster_factory,
            )
            self.assertEqual(sorted(manager.shard_table.keys()), [1])
            manager.refresh_membership(timeout_ms=1000)
            self.assertEqual(sorted(manager.shard_table.keys()), [1, 2])
            self.assertIn(2, manager._clusters)
            self.assertGreaterEqual(len(calls), 3)


if __name__ == "__main__":
    unittest.main()
