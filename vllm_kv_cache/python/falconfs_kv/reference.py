from __future__ import annotations

import time
import uuid
import zlib
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Dict, Iterable, List, Optional, Tuple


class BlockStatus(IntEnum):
    ALLOCATED = 1
    STORED = 2
    EVICTING = 3
    EVICTED = 4
    FAILED = 5


class ErrorCode(str, Enum):
    OK = "OK"
    NOT_FOUND = "NOT_FOUND"
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    CAS_CONFLICT = "CAS_CONFLICT"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    LEASE_TOKEN_MISMATCH = "LEASE_TOKEN_MISMATCH"
    STALE_EPOCH = "STALE_EPOCH"
    STORE_WRITE_FAILED = "STORE_WRITE_FAILED"
    THROTTLED = "THROTTLED"
    CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class StoreRegionState(str, Enum):
    HEALTHY = "HEALTHY"
    DRAINING = "DRAINING"
    SUSPECT = "SUSPECT"
    OFFLINE = "OFFLINE"
    QUARANTINED = "QUARANTINED"


@dataclass
class ItemResult:
    success: bool
    error_code: ErrorCode = ErrorCode.OK
    retryable: bool = False
    error_message: str = ""


@dataclass
class LeaseInfo:
    lease_token: int
    lease_expire_ms: int
    dn_epoch: int
    store_epoch: int


@dataclass
class BlockLocation:
    store_node_id: int
    pool_offset: int
    evicted_path: str = ""
    store_epoch: int = 1


@dataclass
class BlockMeta:
    block_hash: str
    kv_group_idx: int
    layer_mask: int
    status: BlockStatus
    location: BlockLocation
    version: int = 1
    updated_at_ms: int = 0


@dataclass
class StoreRegion:
    store_node_id: int
    owner_dn_id: int
    base_offset: int
    region_bytes: int
    block_size: int
    store_epoch: int = 1
    state: StoreRegionState = StoreRegionState.HEALTHY
    last_heartbeat_ms: int = 0
    bitmap: List[bool] = field(default_factory=list)
    next_hint: int = 0

    def __post_init__(self) -> None:
        total_blocks = self.region_bytes // self.block_size
        if not self.bitmap:
            self.bitmap = [False] * total_blocks

    @property
    def total_blocks(self) -> int:
        return len(self.bitmap)

    @property
    def free_blocks(self) -> int:
        return sum(1 for used in self.bitmap if not used)

    def contains(self, offset: int) -> bool:
        return self.base_offset <= offset < self.base_offset + self.region_bytes

    def allocate(self) -> Tuple[ItemResult, Optional[BlockLocation]]:
        if self.state != StoreRegionState.HEALTHY:
            return ItemResult(False, ErrorCode.THROTTLED, True, "region is not allocating"), None
        if not self.bitmap:
            return ItemResult(False, ErrorCode.INVALID_ARGUMENT, False, "empty region"), None
        for step in range(len(self.bitmap)):
            idx = (self.next_hint + step) % len(self.bitmap)
            if not self.bitmap[idx]:
                self.bitmap[idx] = True
                self.next_hint = (idx + 1) % len(self.bitmap)
                return (
                    ItemResult(True),
                    BlockLocation(
                        store_node_id=self.store_node_id,
                        pool_offset=self.base_offset + idx * self.block_size,
                        store_epoch=self.store_epoch,
                    ),
                )
        return ItemResult(False, ErrorCode.THROTTLED, True, "region exhausted"), None

    def free(self, offset: int) -> ItemResult:
        if not self.contains(offset):
            return ItemResult(False, ErrorCode.INVALID_ARGUMENT, False, "offset outside region")
        relative = offset - self.base_offset
        if relative % self.block_size != 0:
            return ItemResult(False, ErrorCode.INVALID_ARGUMENT, False, "unaligned offset")
        idx = relative // self.block_size
        if idx < 0 or idx >= len(self.bitmap):
            return ItemResult(False, ErrorCode.INVALID_ARGUMENT, False, "offset outside region")
        if not self.bitmap[idx]:
            return ItemResult(False, ErrorCode.INTERNAL_ERROR, False, "double free")
        self.bitmap[idx] = False
        self.next_hint = min(self.next_hint, idx)
        return ItemResult(True)


class StoreRegionRegistry:
    def __init__(self, suspect_ms: int = 3000, offline_ms: int = 10000):
        self._regions: Dict[Tuple[int, int], StoreRegion] = {}
        self.suspect_ms = suspect_ms
        self.offline_ms = offline_ms

    def register(self, region: StoreRegion) -> None:
        self._regions[(region.store_node_id, region.owner_dn_id)] = region

    def for_dn(self, dn_id: int) -> List[StoreRegion]:
        return [r for (_, owner_dn_id), r in self._regions.items() if owner_dn_id == dn_id]

    def find_by_offset(self, store_node_id: int, offset: int) -> Optional[StoreRegion]:
        for (sid, _), region in self._regions.items():
            if sid == store_node_id and region.contains(offset):
                return region
        return None

    def heartbeat(self, store_node_id: int, now_ms: int, store_epoch: Optional[int] = None) -> None:
        for (sid, _), region in self._regions.items():
            if sid != store_node_id:
                continue
            if store_epoch is not None and store_epoch != region.store_epoch:
                region.store_epoch = store_epoch
                region.state = StoreRegionState.QUARANTINED
            elif region.state in (StoreRegionState.SUSPECT, StoreRegionState.OFFLINE):
                region.state = StoreRegionState.HEALTHY
            region.last_heartbeat_ms = now_ms

    def refresh_health(self, now_ms: int) -> None:
        for region in self._regions.values():
            gap = now_ms - region.last_heartbeat_ms
            if gap >= self.offline_ms:
                region.state = StoreRegionState.OFFLINE
            elif gap >= self.suspect_ms:
                region.state = StoreRegionState.SUSPECT


class LeaseManager:
    def __init__(self, default_ttl_ms: int = 5000, dn_epoch: int = 1):
        self.default_ttl_ms = default_ttl_ms
        self.dn_epoch = dn_epoch
        self._leases: Dict[str, LeaseInfo] = {}

    def grant(self, block_hash: str, store_epoch: int, now_ms: int) -> LeaseInfo:
        current = self._leases.get(block_hash)
        expire_ms = now_ms + self.default_ttl_ms
        if current and current.lease_expire_ms > now_ms:
            current.lease_expire_ms = expire_ms
            current.dn_epoch = self.dn_epoch
            current.store_epoch = store_epoch
            return current
        lease = LeaseInfo(
            lease_token=uuid.uuid4().int & ((1 << 63) - 1),
            lease_expire_ms=expire_ms,
            dn_epoch=self.dn_epoch,
            store_epoch=store_epoch,
        )
        self._leases[block_hash] = lease
        return lease

    def renew(
        self,
        block_hash: str,
        lease_token: int,
        expected_dn_epoch: int,
        expected_store_epoch: int,
        now_ms: int,
        ttl_ms: Optional[int] = None,
    ) -> Tuple[ItemResult, Optional[LeaseInfo]]:
        lease = self._leases.get(block_hash)
        if not lease:
            return ItemResult(False, ErrorCode.LEASE_EXPIRED, True, "lease missing"), None
        if expected_dn_epoch != self.dn_epoch or expected_dn_epoch != lease.dn_epoch:
            return ItemResult(False, ErrorCode.STALE_EPOCH, True, "stale dn epoch"), None
        if expected_store_epoch != lease.store_epoch:
            return ItemResult(False, ErrorCode.STALE_EPOCH, True, "stale store epoch"), None
        if lease.lease_token != lease_token:
            return ItemResult(False, ErrorCode.LEASE_TOKEN_MISMATCH, True, "lease token mismatch"), None
        if lease.lease_expire_ms <= now_ms:
            return ItemResult(False, ErrorCode.LEASE_EXPIRED, True, "lease expired"), None
        lease.lease_expire_ms = now_ms + (ttl_ms or self.default_ttl_ms)
        return ItemResult(True), lease

    def can_evict(self, block_hash: str, now_ms: int) -> bool:
        lease = self._leases.get(block_hash)
        return lease is None or lease.lease_expire_ms <= now_ms


class LRUManager:
    def __init__(self):
        self._items: OrderedDict[str, None] = OrderedDict()

    def add_stored(self, block_hash: str) -> None:
        self.touch(block_hash)

    def touch(self, block_hash: str) -> None:
        self._items.pop(block_hash, None)
        self._items[block_hash] = None

    def remove(self, block_hash: str) -> None:
        self._items.pop(block_hash, None)

    def cold_candidates(self, limit: int) -> List[str]:
        return list(self._items.keys())[:limit]


class KVStore:
    def __init__(self, registry: StoreRegionRegistry):
        self.registry = registry
        self._data: Dict[Tuple[int, int], Tuple[bytes, int, int]] = {}

    def write(
        self,
        store_node_id: int,
        pool_offset: int,
        payload: bytes,
        expected_store_epoch: int,
        block_size: int,
        *,
        block_hash: bytes = b"",
        expected_version: int = 0,
        **_kw: object,
    ) -> ItemResult:
        del block_hash, expected_version, _kw
        region = self.registry.find_by_offset(store_node_id, pool_offset)
        if not region:
            return ItemResult(False, ErrorCode.INVALID_ARGUMENT, False, "unknown region")
        if region.state != StoreRegionState.HEALTHY:
            return ItemResult(False, ErrorCode.STORE_WRITE_FAILED, True, "region not healthy")
        if region.store_epoch != expected_store_epoch:
            return ItemResult(False, ErrorCode.STALE_EPOCH, True, "stale store epoch")
        if len(payload) > block_size:
            return ItemResult(False, ErrorCode.INVALID_ARGUMENT, False, "payload too large")
        self._data[(store_node_id, pool_offset)] = (payload, zlib.crc32(payload), region.store_epoch)
        return ItemResult(True)

    def batch_write_blocks(self, store_node_id: int, blocks) -> list:
        return [
            self.write(
                store_node_id,
                b.pool_offset,
                b.payload,
                b.expected_store_epoch,
                b.block_size,
                block_hash=b.block_hash,
                expected_version=b.expected_version,
            )
            for b in blocks
        ]

    def read(
        self,
        store_node_id: int,
        pool_offset: int,
        expected_store_epoch: int,
        expected_version: int = 0,
        block_size: int = 65536,
        *,
        block_hash: bytes = b"",
    ) -> Tuple[ItemResult, bytes]:
        del expected_version, block_size, block_hash
        region = self.registry.find_by_offset(store_node_id, pool_offset)
        if not region:
            return ItemResult(False, ErrorCode.INVALID_ARGUMENT, False, "unknown region"), b""
        if region.store_epoch != expected_store_epoch:
            return ItemResult(False, ErrorCode.STALE_EPOCH, True, "stale store epoch"), b""
        entry = self._data.get((store_node_id, pool_offset))
        if not entry:
            return ItemResult(False, ErrorCode.NOT_FOUND, False, "payload missing"), b""
        payload, _, _ = entry
        return ItemResult(True), payload

    def batch_read_blocks(self, store_node_id: int, blocks) -> list:
        return [
            self.read(
                store_node_id,
                b.pool_offset,
                b.expected_store_epoch,
                b.expected_version,
                b.block_size,
                block_hash=b.block_hash,
            )
            for b in blocks
        ]


class MetadataService:
    def __init__(self, registry: StoreRegionRegistry, dn_id: int = 1, dn_epoch: int = 1):
        self.registry = registry
        self.dn_id = dn_id
        self.dn_epoch = dn_epoch
        self.lease_manager = LeaseManager(dn_epoch=dn_epoch)
        self.lru = LRUManager()
        self.rows: Dict[str, BlockMeta] = {}

    def allocate(
        self,
        block_hashes: Iterable[str],
        now_ms: Optional[int] = None,
        request_id: Optional[str] = None,
        client_id: int = 0,
    ) -> Dict[str, Tuple[ItemResult, Optional[BlockMeta], Optional[LeaseInfo]]]:
        now = now_ms or now_ms_now()
        results: Dict[str, Tuple[ItemResult, Optional[BlockMeta], Optional[LeaseInfo]]] = {}
        for block_hash in block_hashes:
            if block_hash in self.rows:
                row = self.rows[block_hash]
                lease = self.lease_manager.grant(block_hash, row.location.store_epoch, now)
                results[block_hash] = (ItemResult(True), row, lease)
                continue
            regions = sorted(
                self.registry.for_dn(self.dn_id),
                key=lambda r: (r.state != StoreRegionState.HEALTHY, r.free_blocks),
            )
            allocation: Tuple[ItemResult, Optional[BlockLocation]] = (
                ItemResult(False, ErrorCode.THROTTLED, True, "no healthy region"),
                None,
            )
            for region in regions:
                allocation = region.allocate()
                if allocation[0].success:
                    break
            result, location = allocation
            if not result.success or not location:
                results[block_hash] = (result, None, None)
                continue
            row = BlockMeta(
                block_hash=block_hash,
                kv_group_idx=0,
                layer_mask=0,
                status=BlockStatus.ALLOCATED,
                location=location,
                updated_at_ms=now,
            )
            self.rows[block_hash] = row
            lease = self.lease_manager.grant(block_hash, location.store_epoch, now)
            results[block_hash] = (ItemResult(True), row, lease)
        return results

    def lookup(
        self,
        block_hashes: Iterable[str],
        renew_lease_on_hit: bool = True,
        now_ms: Optional[int] = None,
    ) -> Dict[str, Tuple[ItemResult, Optional[BlockMeta], Optional[LeaseInfo]]]:
        now = now_ms or now_ms_now()
        results: Dict[str, Tuple[ItemResult, Optional[BlockMeta], Optional[LeaseInfo]]] = {}
        for block_hash in block_hashes:
            row = self.rows.get(block_hash)
            if not row:
                results[block_hash] = (ItemResult(False, ErrorCode.NOT_FOUND, False, "missing"), None, None)
                continue
            if row.status == BlockStatus.EVICTING:
                results[block_hash] = (ItemResult(False, ErrorCode.CAS_CONFLICT, True, "evicting"), row, None)
                continue
            if row.status not in (BlockStatus.STORED, BlockStatus.EVICTED):
                results[block_hash] = (ItemResult(False, ErrorCode.NOT_FOUND, False, "not cacheable"), row, None)
                continue
            lease = None
            if renew_lease_on_hit and row.status == BlockStatus.STORED:
                lease = self.lease_manager.grant(block_hash, row.location.store_epoch, now)
            results[block_hash] = (ItemResult(True), row, lease)
        return results

    def update_status(
        self,
        block_hash: str,
        expected_from: BlockStatus,
        to_status: BlockStatus,
        expected_version: int,
        evicted_path: str = "",
        request_id: Optional[str] = None,
        client_id: int = 0,
        now_ms: Optional[int] = None,
    ) -> Tuple[ItemResult, Optional[BlockMeta]]:
        now = now_ms or now_ms_now()
        row = self.rows.get(block_hash)
        if not row:
            return (
                ItemResult(False, ErrorCode.NOT_FOUND, False, "missing"),
                None,
            )
        if row.version != expected_version or row.status != expected_from:
            return (
                ItemResult(False, ErrorCode.CAS_CONFLICT, True, "version/status mismatch"),
                row,
            )
        row.status = to_status
        row.version += 1
        row.updated_at_ms = now
        if evicted_path and to_status == BlockStatus.EVICTED:
            row.location.evicted_path = evicted_path
        if to_status == BlockStatus.STORED:
            self.lru.add_stored(block_hash)
        elif to_status in (BlockStatus.EVICTING, BlockStatus.EVICTED, BlockStatus.FAILED):
            self.lru.remove(block_hash)
        if to_status == BlockStatus.EVICTED:
            region = self.registry.find_by_offset(row.location.store_node_id, row.location.pool_offset)
            if region:
                region.free(row.location.pool_offset)
        return (ItemResult(True), row)

    def batch_update_status(
        self,
        updates: List[Tuple[str, BlockStatus, BlockStatus, int, str]],
        *,
        request_id: Optional[str] = None,
        client_id: int = 0,
        now_ms: Optional[int] = None,
    ) -> List[Tuple[ItemResult, Optional[BlockMeta]]]:
        out: List[Tuple[ItemResult, Optional[BlockMeta]]] = []
        for block_hash, ef, to_st, exp_ver, ev_path in updates:
            out.append(
                self.update_status(
                    block_hash,
                    ef,
                    to_st,
                    exp_ver,
                    evicted_path=ev_path,
                    request_id=request_id,
                    client_id=client_id,
                    now_ms=now_ms,
                )
            )
        return out

    def free_allocated(
        self,
        block_hash: str,
        expected_version: int,
        force: bool = False,
        request_id: Optional[str] = None,
        client_id: int = 0,
        now_ms: Optional[int] = None,
    ) -> Tuple[ItemResult, Optional[int]]:
        """Free a previously ALLOCATED row and its bitmap slot.

        Mirrors `BatchFreeAllocated` semantics: only ALLOCATED or FAILED rows are freed
        unless `force=True`. The bitmap slot is released, lease entry removed, and the
        metadata row deleted.
        """
        now = now_ms or now_ms_now()
        row = self.rows.get(block_hash)
        if not row:
            return (
                ItemResult(False, ErrorCode.NOT_FOUND, False, "missing"),
                None,
            )
        if row.version != expected_version:
            return (
                ItemResult(False, ErrorCode.CAS_CONFLICT, True, "version mismatch"),
                row.version,
            )
        if not force and row.status not in (BlockStatus.ALLOCATED, BlockStatus.FAILED):
            return (
                ItemResult(False, ErrorCode.INVALID_ARGUMENT, False, "row not freeable"),
                row.version,
            )
        region = self.registry.find_by_offset(row.location.store_node_id, row.location.pool_offset)
        if region:
            region.free(row.location.pool_offset)
        self.lease_manager._leases.pop(block_hash, None)
        self.lru.remove(block_hash)
        del self.rows[block_hash]
        return (ItemResult(True), expected_version + 1)

    def begin_eviction(
        self,
        block_hash: str,
        expected_version: int,
        request_id: Optional[str] = None,
        client_id: int = 0,
        now_ms: Optional[int] = None,
    ) -> Tuple[ItemResult, Optional[BlockMeta]]:
        """Phase 1 of two-phase eviction: CAS STORED -> EVICTING."""
        return self.update_status(
            block_hash,
            BlockStatus.STORED,
            BlockStatus.EVICTING,
            expected_version,
            request_id=request_id,
            client_id=client_id,
            now_ms=now_ms,
        )

    def commit_eviction(
        self,
        block_hash: str,
        expected_version: int,
        evicted_path: str,
        request_id: Optional[str] = None,
        client_id: int = 0,
        now_ms: Optional[int] = None,
    ) -> Tuple[ItemResult, Optional[BlockMeta]]:
        """Phase 2 success: CAS EVICTING -> EVICTED, set evicted_path, free bitmap."""
        if not evicted_path:
            return ItemResult(False, ErrorCode.INVALID_ARGUMENT, False, "evicted_path required"), None
        return self.update_status(
            block_hash,
            BlockStatus.EVICTING,
            BlockStatus.EVICTED,
            expected_version,
            evicted_path=evicted_path,
            request_id=request_id,
            client_id=client_id,
            now_ms=now_ms,
        )

    def rollback_eviction(
        self,
        block_hash: str,
        expected_version: int,
        request_id: Optional[str] = None,
        client_id: int = 0,
        now_ms: Optional[int] = None,
    ) -> Tuple[ItemResult, Optional[BlockMeta]]:
        """Phase 2 failure: CAS EVICTING -> STORED."""
        return self.update_status(
            block_hash,
            BlockStatus.EVICTING,
            BlockStatus.STORED,
            expected_version,
            request_id=request_id,
            client_id=client_id,
            now_ms=now_ms,
        )


class ReferenceCluster:
    def __init__(self, block_size: int = 65536):
        now = now_ms_now()
        self.registry = StoreRegionRegistry()
        self.registry.register(
            StoreRegion(
                store_node_id=0,
                owner_dn_id=1,
                base_offset=0,
                region_bytes=block_size * 1024,
                block_size=block_size,
                last_heartbeat_ms=now,
            )
        )
        self.metadata = MetadataService(self.registry)
        self.store = KVStore(self.registry)
        self.block_size = block_size


def now_ms_now() -> int:
    return int(time.time() * 1000)
