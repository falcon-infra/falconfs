#!/usr/bin/env python3
"""Multi-DN routing and partial-failure cleanup tests for FalconFSOffloadingManager.

Covers acceptance items A8 (complete_store cleanup) and A12 (multi-DN
distribution); the same harness will also verify these against the real BRPC
cluster once cluster mode lands (M3/M4).
"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv import FalconFSOffloadingManager  # noqa: E402
from falconfs_kv.offloading_manager import _route_dn_id  # noqa: E402
from falconfs_kv.reference import (  # noqa: E402
    BlockStatus,
    ReferenceCluster,
    StoreRegion,
    StoreRegionRegistry,
)


def _build_cluster_for_dn(dn_id: int) -> ReferenceCluster:
    cluster = ReferenceCluster(block_size=1024)
    # ReferenceCluster default registers a region for dn 1; rebuild for the actual DN id.
    cluster.registry = StoreRegionRegistry()
    cluster.registry.register(
        StoreRegion(
            store_node_id=dn_id,
            owner_dn_id=dn_id,
            base_offset=0,
            region_bytes=4 * 1024,
            block_size=1024,
            last_heartbeat_ms=0,
        )
    )
    cluster.metadata.registry = cluster.registry
    cluster.metadata.dn_id = dn_id
    cluster.store.registry = cluster.registry
    return cluster


def _find_keys_routed_to(shard_table, dn_id, candidate_count=200):
    keys = []
    for i in range(candidate_count):
        key = f"k{i}"
        if _route_dn_id(shard_table, key) == dn_id:
            keys.append(key)
        if len(keys) == 2:
            break
    if len(keys) < 2:
        raise RuntimeError("could not synthesize keys for the requested DN")
    return keys


class MultiDNRoutingTest(unittest.TestCase):
    def setUp(self):
        self.shard_table = {0: "127.0.0.1:55520", 1: "127.0.0.1:55540"}
        self.clusters = {dn_id: _build_cluster_for_dn(dn_id) for dn_id in self.shard_table}
        self.manager = FalconFSOffloadingManager(
            shard_table=self.shard_table,
            client_id=7,
            client_hostname="localhost",
            clusters=self.clusters,
            block_size=1024,
        )

    def test_keys_split_across_dns_each_dn_holds_its_share(self):
        dn0_keys = _find_keys_routed_to(self.shard_table, 0)
        dn1_keys = _find_keys_routed_to(self.shard_table, 1)
        keys = dn0_keys + dn1_keys
        spec = self.manager.prepare_store(keys)
        self.assertIsNotNone(spec)
        data = {k: b"x" * 16 for k in keys}
        self.manager.complete_store(keys, data)

        for k in dn0_keys:
            self.assertIn(k, self.clusters[0].metadata.rows)
            self.assertNotIn(k, self.clusters[1].metadata.rows)
        for k in dn1_keys:
            self.assertIn(k, self.clusters[1].metadata.rows)
            self.assertNotIn(k, self.clusters[0].metadata.rows)

    def test_complete_store_failure_frees_allocation(self):
        keys = _find_keys_routed_to(self.shard_table, 0)
        self.manager.prepare_store(keys)
        target_cluster = self.clusters[0]
        self.assertEqual(len(target_cluster.metadata.rows), len(keys))

        self.manager.complete_store(keys, success=False)
        self.assertEqual(len(target_cluster.metadata.rows), 0)
        region = target_cluster.registry.for_dn(0)[0]
        self.assertEqual(region.free_blocks, region.total_blocks)

    def test_partial_complete_store_frees_only_failed_keys(self):
        dn0_keys = _find_keys_routed_to(self.shard_table, 0)
        good, bad = dn0_keys
        self.manager.prepare_store(dn0_keys)
        # Only `good` has payload; `bad` will not be written and must be freed.
        self.manager.complete_store(dn0_keys, {good: b"hi"})

        rows = self.clusters[0].metadata.rows
        self.assertIn(good, rows)
        self.assertEqual(rows[good].status, BlockStatus.STORED)
        self.assertNotIn(bad, rows)

    def test_prepare_store_returns_none_when_all_allocations_fail(self):
        # Mark every region as offline so allocations cannot succeed.
        from falconfs_kv.reference import StoreRegionState

        for cluster in self.clusters.values():
            for region in cluster.registry.for_dn(cluster.metadata.dn_id):
                region.state = StoreRegionState.OFFLINE

        spec = self.manager.prepare_store(["a", "b", "c"])
        self.assertIsNone(spec)

    def test_cluster_factory_builds_per_dn_clusters(self):
        calls = []

        def build(dn_id, endpoint):
            calls.append((dn_id, endpoint))
            return _build_cluster_for_dn(dn_id)

        manager = FalconFSOffloadingManager(
            shard_table=self.shard_table,
            client_id=8,
            client_hostname="localhost",
            cluster_factory=build,
            block_size=1024,
        )
        self.assertEqual(calls, [(0, "127.0.0.1:55520"), (1, "127.0.0.1:55540")])
        spec = manager.prepare_store(["k0", "k1", "k2"])
        self.assertIsNotNone(spec)
        # If cluster mode is active, each stored location carries a routed DN id.
        self.assertTrue(all(item["dn_id"] in self.shard_table for item in spec.specs))


if __name__ == "__main__":
    unittest.main()
