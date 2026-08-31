"""BRPC-backed DN metadata client (v6 §10/§11 / M3).

`BrpcMetadataService` is a duck-type drop-in for `reference.MetadataService`:
it exposes the exact methods that `FalconFSOffloadingManager` invokes
(`allocate`, `lookup`, `update_status`, `batch_update_status`, `free_allocated`, plus
`lease_manager.renew`) but each method translates to a single BRPC RPC
against the live cluster's KVMetadataService endpoint.

The Python-protobuf marshaling lives here so the OffloadingManager can stay
oblivious to the wire format.
"""

from __future__ import annotations

import os
import re
import threading
import zlib
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from . import falconfs_kv_brpc
from . import kv_common_pb2 as _kvcommon
from . import kv_metadata_service_pb2 as _kvmeta
from .reference import (
    BlockLocation,
    BlockMeta,
    BlockStatus,
    ErrorCode,
    ItemResult,
    LeaseInfo,
    now_ms_now,
)

def _default_preferred_store_id() -> int:
    """Align Python clients with v6.5 mixed-colocation harness (NODE_NAME=v65mixK -> store K+1).

    Falls back to ``FALCON_KV_PREFERRED_STORE_ID`` or ``1`` when unset / unparsable.
    """
    node = os.environ.get("NODE_NAME", "").strip()
    m = re.fullmatch(r"v65mix(\d+)", node)
    if m:
        return int(m.group(1)) + 1
    raw = os.environ.get("FALCON_KV_PREFERRED_STORE_ID", "1")
    try:
        return int(raw)
    except ValueError:
        return 1


def _preferred_store_ids_for_allocation() -> List[int]:
    """Return preferred Store ids for allocation.

    Colocated clients keep v6 locality affinity by default. All-remote clients
    stripe across discovered Stores so large remote batches do not funnel through
    one Store. Operators can force striping with FALCON_KV_STRIPE_PREFERRED_STORES
    or provide an explicit FALCON_KV_PREFERRED_STORE_IDS list.
    """
    raw = os.environ.get("FALCON_KV_PREFERRED_STORE_IDS", "").strip()
    ids: List[int] = []
    if raw:
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                sid = int(part)
            except ValueError:
                continue
            if sid > 0 and sid not in ids:
                ids.append(sid)
        if ids:
            return ids

    stripe_raw = os.environ.get("FALCON_KV_STRIPE_PREFERRED_STORES", "").strip().lower()
    force_stripe = stripe_raw in ("1", "true", "yes", "on")
    # NODE_NAME=v65mixK means this client is colocated with Store K+1. Preserve
    # that SHM-first affinity unless the caller explicitly asks to stripe.
    if os.environ.get("NODE_NAME", "").strip() and not force_stripe:
        return [_default_preferred_store_id()]

    try:
        loc = falconfs_kv_brpc.store_locality()
        ids = sorted({int(k) for k in dict(loc).keys() if int(k) > 0})
    except Exception:
        ids = []
    if not ids:
        ids = [_default_preferred_store_id()]
    return ids


def _preferred_store_id_for_block(block_hash, store_ids: List[int]) -> int:
    if not store_ids:
        return _default_preferred_store_id()
    if len(store_ids) == 1:
        return store_ids[0]
    return store_ids[zlib.crc32(_to_bytes(block_hash)) % len(store_ids)]


# ---------------------------------------------------------------------------
# Wire conversion helpers
# ---------------------------------------------------------------------------

_PROTO_TO_REF_STATUS = {
    _kvcommon.BLOCK_STATUS_UNSPECIFIED: BlockStatus.ALLOCATED,
    _kvcommon.BLOCK_STATUS_ALLOCATED: BlockStatus.ALLOCATED,
    _kvcommon.BLOCK_STATUS_STORED: BlockStatus.STORED,
    _kvcommon.BLOCK_STATUS_EVICTING: BlockStatus.EVICTING,
    _kvcommon.BLOCK_STATUS_EVICTED: BlockStatus.EVICTED,
    _kvcommon.BLOCK_STATUS_FAILED: BlockStatus.FAILED,
}

_REF_TO_PROTO_STATUS = {
    BlockStatus.ALLOCATED: _kvcommon.BLOCK_STATUS_ALLOCATED,
    BlockStatus.STORED: _kvcommon.BLOCK_STATUS_STORED,
    BlockStatus.EVICTING: _kvcommon.BLOCK_STATUS_EVICTING,
    BlockStatus.EVICTED: _kvcommon.BLOCK_STATUS_EVICTED,
    BlockStatus.FAILED: _kvcommon.BLOCK_STATUS_FAILED,
}

_PROTO_ERROR_TO_REF = {
    _kvcommon.OK: ErrorCode.OK,
    _kvcommon.NOT_FOUND: ErrorCode.NOT_FOUND,
    _kvcommon.INVALID_ARGUMENT: ErrorCode.INVALID_ARGUMENT,
    _kvcommon.CAS_CONFLICT: ErrorCode.CAS_CONFLICT,
    _kvcommon.LEASE_EXPIRED: ErrorCode.LEASE_EXPIRED,
    _kvcommon.LEASE_TOKEN_MISMATCH: ErrorCode.LEASE_TOKEN_MISMATCH,
    _kvcommon.STALE_EPOCH: ErrorCode.STALE_EPOCH,
    _kvcommon.STORE_WRITE_FAILED: ErrorCode.STORE_WRITE_FAILED,
    _kvcommon.THROTTLED: ErrorCode.THROTTLED,
    _kvcommon.CHECKSUM_MISMATCH: ErrorCode.CHECKSUM_MISMATCH,
    _kvcommon.INTERNAL_ERROR: ErrorCode.INTERNAL_ERROR,
}


def _to_str(block_hash) -> str:
    if isinstance(block_hash, bytes):
        try:
            return block_hash.decode("utf-8")
        except UnicodeDecodeError:
            return block_hash.hex()
    return str(block_hash)


def _to_bytes(block_hash) -> bytes:
    if isinstance(block_hash, bytes):
        return block_hash
    return str(block_hash).encode("utf-8")


def _result_meta_to_item_result(meta) -> ItemResult:
    return ItemResult(
        success=bool(meta.success),
        error_code=_PROTO_ERROR_TO_REF.get(meta.error_code, ErrorCode.INTERNAL_ERROR),
        retryable=bool(meta.retryable),
        error_message=meta.error_message or "",
    )


def _proto_to_blockmeta(block_hash: str, status_int: int, location_proto, version: int) -> BlockMeta:
    location = BlockLocation(
        store_node_id=location_proto.store_node_id,
        pool_offset=location_proto.pool_offset,
        evicted_path=location_proto.evicted_path or "",
        store_epoch=location_proto.store_epoch,
    )
    return BlockMeta(
        block_hash=block_hash,
        kv_group_idx=0,
        layer_mask=0,
        status=_PROTO_TO_REF_STATUS.get(status_int, BlockStatus.ALLOCATED),
        location=location,
        version=version,
    )


def _proto_to_lease(lease_proto) -> LeaseInfo:
    return LeaseInfo(
        lease_token=lease_proto.lease_token,
        lease_expire_ms=lease_proto.lease_expire_ms,
        dn_epoch=lease_proto.dn_epoch,
        store_epoch=lease_proto.store_epoch,
    )


# ---------------------------------------------------------------------------
# BRPC-backed lease shim
# ---------------------------------------------------------------------------


class _BrpcLeaseManager:
    """Duck-type of `reference.LeaseManager`: only the `renew` method is used by
    the OffloadingManager. Every other method on the reference lease manager
    operates on in-memory state that does not exist in the live cluster."""

    def __init__(self, parent: "BrpcMetadataService"):
        self._parent = parent

    def renew(
        self,
        block_hash: str,
        lease_token: int,
        expected_dn_epoch: int,
        expected_store_epoch: int,
        now_ms: int,
        requested_ttl_ms: int = 5000,
    ) -> Tuple[ItemResult, Optional[LeaseInfo]]:
        del now_ms  # server-side now is authoritative
        req = _kvmeta.BatchRenewLeaseRequest()
        req.meta.request_id = self._parent._mk_request_id("renew")
        req.meta.client_id = self._parent.client_id
        req.requested_ttl_ms = requested_ttl_ms
        item = req.items.add()
        item.block_hash = _to_bytes(block_hash)
        item.lease_token = lease_token
        item.expected_dn_epoch = expected_dn_epoch
        item.expected_store_epoch = expected_store_epoch
        resp = _kvmeta.BatchRenewLeaseResponse()
        resp.ParseFromString(
            falconfs_kv_brpc.batch_renew_lease(
                self._parent.endpoint, req.SerializeToString(), self._parent.timeout_ms
            )
        )
        if not resp.results:
            return (
                ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "empty response"),
                None,
            )
        r = resp.results[0]
        item_result = _result_meta_to_item_result(r.result)
        lease = (
            _proto_to_lease(r.lease) if r.HasField("lease") and item_result.success else None
        )
        return item_result, lease


# ---------------------------------------------------------------------------
# Top-level metadata service shim
# ---------------------------------------------------------------------------


class BrpcMetadataService:
    """Minimal BRPC client wired to the methods OffloadingManager depends on.

    Mirrors `reference.MetadataService` so a `BrpcCluster` can be passed
    through the existing `clusters=` parameter on FalconFSOffloadingManager.
    """

    def __init__(self, endpoint: str, *, dn_id: int = 1, client_id: int = 0,
                 timeout_ms: int = 30000, block_size: int = 65536):
        self.endpoint = endpoint
        self.dn_id = dn_id
        self.client_id = client_id
        self.timeout_ms = timeout_ms
        self.block_size = block_size
        self.lease_manager = _BrpcLeaseManager(self)
        self._req_seq = 0
        self._req_lock = threading.Lock()

    # OffloadingManager keeps a Python-side `rows` mirror via `local_cache`,
    # so the cluster does not need a public `rows` dict; we expose an empty
    # one so any debug code that probes it stays happy.
    rows: Dict[str, BlockMeta] = {}

    # -- Helpers --

    def _mk_request_id(self, tag: str) -> str:
        with self._req_lock:
            self._req_seq += 1
            return f"py_dn_client_{tag}_{self.dn_id}_{self._req_seq}"

    # -- Public API mirroring MetadataService --

    def allocate(
        self,
        block_hashes: Iterable[str],
        now_ms: Optional[int] = None,
        request_id: Optional[str] = None,
        client_id: Optional[int] = None,
        allocate_hint: Optional[int] = None,
    ) -> Dict[str, Tuple[ItemResult, Optional[BlockMeta], Optional[LeaseInfo]]]:
        del now_ms  # server-authoritative
        items: List[str] = list(block_hashes)
        results: Dict[str, Tuple[ItemResult, Optional[BlockMeta], Optional[LeaseInfo]]] = {}
        if not items:
            return results

        req = _kvmeta.BatchAllocateRequest()
        req.meta.request_id = request_id or self._mk_request_id("alloc")
        req.meta.client_id = client_id if client_id is not None else self.client_id
        req.deduplicate_in_request = True
        preferred_store_ids = _preferred_store_ids_for_allocation()
        for h in items:
            it = req.items.add()
            it.block_hash = _to_bytes(h)
            it.block_size = self.block_size
            it.preferred_store_id = _preferred_store_id_for_block(h, preferred_store_ids)
            it.allow_fallback_store = True
            if allocate_hint is not None:
                it.allocate_hint = allocate_hint
        rsp = _kvmeta.BatchAllocateResponse()
        rsp.ParseFromString(
            falconfs_kv_brpc.batch_allocate_with_lease(
                self.endpoint, req.SerializeToString(), self.timeout_ms
            )
        )
        for it, r in zip(items, rsp.results):
            ir = _result_meta_to_item_result(r.result)
            row = None
            lease = None
            if ir.success:
                row = _proto_to_blockmeta(_to_str(it),
                                          int(_kvcommon.BLOCK_STATUS_ALLOCATED),
                                          r.location, r.version)
                if r.HasField("lease"):
                    lease = _proto_to_lease(r.lease)
            results[_to_str(it)] = (ir, row, lease)
        return results

    def lookup(
        self,
        block_hashes: Iterable[str],
        renew_lease_on_hit: bool = True,
        now_ms: Optional[int] = None,
    ) -> Dict[str, Tuple[ItemResult, Optional[BlockMeta], Optional[LeaseInfo]]]:
        del now_ms
        items: List[str] = list(block_hashes)
        results: Dict[str, Tuple[ItemResult, Optional[BlockMeta], Optional[LeaseInfo]]] = {}
        if not items:
            return results
        req = _kvmeta.BatchLookupRequest()
        req.meta.request_id = self._mk_request_id("lookup")
        req.meta.client_id = self.client_id
        for h in items:
            it = req.items.add()
            it.block_hash = _to_bytes(h)
            it.renew_lease_on_hit = renew_lease_on_hit
        rsp = _kvmeta.BatchLookupResponse()
        rsp.ParseFromString(
            falconfs_kv_brpc.batch_lookup_with_lease(
                self.endpoint, req.SerializeToString(), self.timeout_ms
            )
        )
        for h, r in zip(items, rsp.results):
            ir = _result_meta_to_item_result(r.result)
            row = None
            lease = None
            if ir.success and r.HasField("location"):
                row = _proto_to_blockmeta(_to_str(h), int(r.status), r.location, r.version)
                if r.HasField("lease"):
                    lease = _proto_to_lease(r.lease)
            results[_to_str(h)] = (ir, row, lease)
        return results

    def update_status(
        self,
        block_hash: str,
        expected_from: BlockStatus,
        to_status: BlockStatus,
        expected_version: int,
        evicted_path: str = "",
        request_id: Optional[str] = None,
        client_id: Optional[int] = None,
        now_ms: Optional[int] = None,
    ) -> Tuple[ItemResult, Optional[BlockMeta]]:
        del now_ms
        req = _kvmeta.BatchUpdateStatusRequest()
        req.meta.request_id = request_id or self._mk_request_id("upd")
        req.meta.client_id = client_id if client_id is not None else self.client_id
        it = req.items.add()
        it.block_hash = _to_bytes(block_hash)
        it.expected_from_status = _REF_TO_PROTO_STATUS[expected_from]
        it.to_status = _REF_TO_PROTO_STATUS[to_status]
        it.expected_version = expected_version
        if evicted_path:
            it.evicted_path = evicted_path
        rsp = _kvmeta.BatchUpdateStatusResponse()
        rsp.ParseFromString(
            falconfs_kv_brpc.batch_update_block_status(
                self.endpoint, req.SerializeToString(), self.timeout_ms
            )
        )
        if not rsp.results:
            return (ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "empty response"), None)
        r = rsp.results[0]
        ir = _result_meta_to_item_result(r.result)
        if not ir.success:
            return ir, None
        # Reconstruct BlockMeta with the post-update version + status.
        loc = BlockLocation(0, 0, "", 1)  # location unchanged by status update
        row = BlockMeta(
            block_hash=_to_str(block_hash),
            kv_group_idx=0,
            layer_mask=0,
            status=_PROTO_TO_REF_STATUS.get(int(r.current_status), to_status),
            location=loc,
            version=r.new_version,
        )
        return ir, row

    def batch_update_status(
        self,
        updates: List[Tuple[str, BlockStatus, BlockStatus, int, str]],
        *,
        request_id: Optional[str] = None,
        client_id: Optional[int] = None,
        now_ms: Optional[int] = None,
    ) -> List[Tuple[ItemResult, Optional[BlockMeta]]]:
        del now_ms
        if not updates:
            return []
        req = _kvmeta.BatchUpdateStatusRequest()
        req.meta.request_id = request_id or self._mk_request_id("batch_upd")
        req.meta.client_id = client_id if client_id is not None else self.client_id
        for block_hash, expected_from, to_status, expected_version, evicted_path in updates:
            it = req.items.add()
            it.block_hash = _to_bytes(block_hash)
            it.expected_from_status = _REF_TO_PROTO_STATUS[expected_from]
            it.to_status = _REF_TO_PROTO_STATUS[to_status]
            it.expected_version = expected_version
            if evicted_path:
                it.evicted_path = evicted_path
        rsp = _kvmeta.BatchUpdateStatusResponse()
        rsp.ParseFromString(
            falconfs_kv_brpc.batch_update_block_status(
                self.endpoint, req.SerializeToString(), self.timeout_ms
            )
        )
        out: List[Tuple[ItemResult, Optional[BlockMeta]]] = []
        for i, (block_hash, _ef, to_status, _ev, _ep) in enumerate(updates):
            if i >= len(rsp.results):
                out.append((ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "missing result"), None))
                continue
            r = rsp.results[i]
            ir = _result_meta_to_item_result(r.result)
            if not ir.success:
                out.append((ir, None))
                continue
            loc = BlockLocation(0, 0, "", 1)
            row = BlockMeta(
                block_hash=block_hash,
                kv_group_idx=0,
                layer_mask=0,
                status=_PROTO_TO_REF_STATUS.get(int(r.current_status), to_status),
                location=loc,
                version=r.new_version,
            )
            out.append((ir, row))
        return out

    def renew_many(
        self,
        renewals: List[Tuple[str, int, int, int]],
        *,
        requested_ttl_ms: int = 5000,
    ) -> Dict[str, Tuple[ItemResult, Optional[LeaseInfo]]]:
        """Batch lease renewals for one DN in one metadata RPC."""
        if not renewals:
            return {}
        req = _kvmeta.BatchRenewLeaseRequest()
        req.meta.request_id = self._mk_request_id("batch_renew")
        req.meta.client_id = self.client_id
        req.requested_ttl_ms = requested_ttl_ms
        for block_hash, lease_token, expected_dn_epoch, expected_store_epoch in renewals:
            it = req.items.add()
            it.block_hash = _to_bytes(block_hash)
            it.lease_token = lease_token
            it.expected_dn_epoch = expected_dn_epoch
            it.expected_store_epoch = expected_store_epoch
        rsp = _kvmeta.BatchRenewLeaseResponse()
        rsp.ParseFromString(
            falconfs_kv_brpc.batch_renew_lease(
                self.endpoint, req.SerializeToString(), self.timeout_ms
            )
        )
        out: Dict[str, Tuple[ItemResult, Optional[LeaseInfo]]] = {}
        for i, (block_hash, _token, _dn_epoch, _store_epoch) in enumerate(renewals):
            if i >= len(rsp.results):
                out[block_hash] = (
                    ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "missing result"),
                    None,
                )
                continue
            r = rsp.results[i]
            ir = _result_meta_to_item_result(r.result)
            lease = _proto_to_lease(r.lease) if ir.success and r.HasField("lease") else None
            out[block_hash] = (ir, lease)
        return out

    def free_allocated(
        self,
        block_hash: str,
        expected_version: int,
        force: bool = False,
        request_id: Optional[str] = None,
        client_id: Optional[int] = None,
        now_ms: Optional[int] = None,
    ) -> Tuple[ItemResult, Optional[int]]:
        del now_ms
        req = _kvmeta.BatchFreeAllocatedRequest()
        req.meta.request_id = request_id or self._mk_request_id("free")
        req.meta.client_id = client_id if client_id is not None else self.client_id
        it = req.items.add()
        it.block_hash = _to_bytes(block_hash)
        it.expected_version = expected_version
        it.force = force
        rsp = _kvmeta.BatchFreeAllocatedResponse()
        rsp.ParseFromString(
            falconfs_kv_brpc.batch_free_allocated(
                self.endpoint, req.SerializeToString(), self.timeout_ms
            )
        )
        if not rsp.results:
            return (ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "empty response"), None)
        r = rsp.results[0]
        ir = _result_meta_to_item_result(r.result)
        return ir, (r.new_version if ir.success else None)

    def batch_free_allocated(
        self,
        frees: List[Tuple[str, int, bool]],
        *,
        request_id: Optional[str] = None,
        client_id: Optional[int] = None,
        now_ms: Optional[int] = None,
    ) -> List[Tuple[ItemResult, Optional[int]]]:
        del now_ms
        if not frees:
            return []
        req = _kvmeta.BatchFreeAllocatedRequest()
        req.meta.request_id = request_id or self._mk_request_id("batch_free")
        req.meta.client_id = client_id if client_id is not None else self.client_id
        for block_hash, expected_version, force in frees:
            it = req.items.add()
            it.block_hash = _to_bytes(block_hash)
            it.expected_version = expected_version
            it.force = force
        rsp = _kvmeta.BatchFreeAllocatedResponse()
        rsp.ParseFromString(
            falconfs_kv_brpc.batch_free_allocated(
                self.endpoint, req.SerializeToString(), self.timeout_ms
            )
        )
        out: List[Tuple[ItemResult, Optional[int]]] = []
        for i, _free in enumerate(frees):
            if i >= len(rsp.results):
                out.append((ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "missing result"), None))
                continue
            r = rsp.results[i]
            ir = _result_meta_to_item_result(r.result)
            out.append((ir, r.new_version if ir.success else None))
        return out
