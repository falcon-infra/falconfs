"""Shard-table router for the FalconFS KV cache OffloadingManager.

v6 §3.0 distribution model: each block hash maps deterministically to a single
DN. Until the production shard-table is wired in, we use a stable Python
hash modulo the sorted shard-table keys. The sole purpose of this module is
to keep the routing policy in one place so cluster-mode and reference-mode
managers behave identically.
"""

from __future__ import annotations

from typing import Dict, Iterable, List


class Router:
    def __init__(self, shard_table: Dict[int, str]):
        if not shard_table:
            raise RuntimeError("Router: empty shard_table; at least one DN must be registered")
        self._shard_table = dict(shard_table)
        self._sorted_dn_ids: List[int] = sorted(self._shard_table.keys())
        # Cache (block_hash -> dn_id) so subsequent calls reuse the same DN.
        self._routing: Dict[str, int] = {}

    @property
    def shard_table(self) -> Dict[int, str]:
        return dict(self._shard_table)

    @property
    def dn_ids(self) -> List[int]:
        return list(self._sorted_dn_ids)

    def endpoint_for(self, dn_id: int) -> str:
        return self._shard_table[dn_id]

    def route(self, block_hash: str) -> int:
        cached = self._routing.get(block_hash)
        if cached is not None:
            return cached
        dn_id = self._sorted_dn_ids[hash(block_hash) % len(self._sorted_dn_ids)]
        self._routing[block_hash] = dn_id
        return dn_id

    def group_by_dn(self, keys: Iterable[str]) -> Dict[int, List[str]]:
        grouped: Dict[int, List[str]] = {}
        for key in keys:
            grouped.setdefault(self.route(key), []).append(key)
        return grouped

    def forget(self, block_hash: str) -> None:
        self._routing.pop(block_hash, None)
