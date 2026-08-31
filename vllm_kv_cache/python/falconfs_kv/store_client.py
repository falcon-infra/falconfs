"""BRPC-backed Store data client (v6 §12 / M3).

Duck-type drop-in for `reference.KVStore` exposing ``write`` and ``read``,
which is what the OffloadingManager invokes. Spilled blocks are read only
through ``KVDataService.BatchReadFromSSD`` via :meth:`read_from_ssd` (never
by opening ``evicted_path`` through the FalconFS file client in the vLLM
process — v6.6.2 design).
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple

from . import falconfs_kv_brpc
from . import kv_data_service_pb2 as _kvdata
from .dn_client import _result_meta_to_item_result
from .reference import ErrorCode, ItemResult

def _store_rpc_retry(fn, endpoint: str, req_bytes: bytes, timeout_ms: int) -> bytes:
    """Store BRPC call with short backoff on transient ``E112`` / not-connected."""
    last: BaseException | None = None
    for i in range(12):
        try:
            return fn(endpoint, req_bytes, timeout_ms)
        except RuntimeError as e:
            last = e
            msg = str(e)
            if i == 11 or ("Not connected" not in msg and "E112" not in msg):
                raise
            time.sleep(0.05 * float(min(1 + i, 24)))
    assert last is not None
    raise last



def _facade_rpc_retry(fn, *args):
    """Facade call retry for pooled BRPC channels warming up under parallel load."""
    last: BaseException | None = None
    for i in range(12):
        try:
            return fn(*args)
        except RuntimeError as e:
            last = e
            msg = str(e)
            if i == 11 or ("Not connected" not in msg and "E112" not in msg):
                raise
            time.sleep(0.05 * float(min(1 + i, 24)))
    assert last is not None
    raise last

def _write_block_retry(endpoint: str, req_bytes: bytes, timeout_ms: int) -> bytes:
    return _store_rpc_retry(falconfs_kv_brpc.write_block, endpoint, req_bytes, timeout_ms)


def _read_block_retry(endpoint: str, req_bytes: bytes, timeout_ms: int) -> bytes:
    return _store_rpc_retry(falconfs_kv_brpc.read_block, endpoint, req_bytes, timeout_ms)


def _read_from_ssd_retry(endpoint: str, req_bytes: bytes, timeout_ms: int) -> bytes:
    return _store_rpc_retry(falconfs_kv_brpc.read_from_ssd, endpoint, req_bytes, timeout_ms)


def _batch_write_block_retry(endpoint: str, req_bytes: bytes, timeout_ms: int) -> bytes:
    return _store_rpc_retry(falconfs_kv_brpc.batch_write_block, endpoint, req_bytes, timeout_ms)


def _batch_read_block_retry(endpoint: str, req_bytes: bytes, timeout_ms: int) -> bytes:
    return _store_rpc_retry(falconfs_kv_brpc.batch_read_block, endpoint, req_bytes, timeout_ms)


def _batch_read_from_ssd_retry(endpoint: str, req_bytes: bytes, timeout_ms: int) -> bytes:
    return _store_rpc_retry(falconfs_kv_brpc.batch_read_from_ssd, endpoint, req_bytes, timeout_ms)


@dataclass(frozen=True)
class StoreBlockWrite:
    """One logical KV block for a batched ``BatchWriteBlock`` RPC."""

    pool_offset: int
    payload: Any
    block_hash: bytes
    block_size: int
    expected_store_epoch: int
    expected_version: int = 0


@dataclass(frozen=True)
class StoreBlockRead:
    """One logical KV block for a batched ``BatchReadBlock`` RPC."""

    pool_offset: int
    block_hash: bytes
    block_size: int
    expected_store_epoch: int
    expected_version: int = 0


class BrpcKVStore:
    def __init__(self, endpoint: str, *, timeout_ms: int = 30000, use_facade_registry: bool = False):
        self.endpoint = endpoint
        self.timeout_ms = timeout_ms
        self._req_seq = 0
        self._req_lock = threading.Lock()
        self._use_facade_registry = use_facade_registry

    def _mk_request_id(self, tag: str) -> str:
        with self._req_lock:
            self._req_seq += 1
            return f"py_store_client_{tag}_{self._req_seq}"

    def write(
        self,
        store_node_id: int,
        pool_offset: int,
        payload: Any,
        expected_store_epoch: int,
        block_size: int,
        expected_version: int = 0,
        *,
        block_hash: bytes = b"",
    ) -> ItemResult:
        req = _kvdata.WriteBlockRequest()
        req.meta.request_id = self._mk_request_id("write")
        it = req.item
        it.block_hash = block_hash
        it.pool_offset = pool_offset
        it.block_size = block_size
        it.expected_store_epoch = expected_store_epoch
        it.expected_version = expected_version
        rsp = _kvdata.WriteBlockResponse()
        wire = b""
        if self._use_facade_registry and hasattr(falconfs_kv_brpc, "facade_write_block_payload"):
            try:
                it.ClearField("payload")
                wire = _facade_rpc_retry(
                    falconfs_kv_brpc.facade_write_block_payload,
                    int(store_node_id), req.SerializeToString(), payload
                )
            except Exception:
                wire = b""
            finally:
                pass
        if self._use_facade_registry and not wire:
            try:
                it.payload = bytes(payload)
                wire = _facade_rpc_retry(
                    falconfs_kv_brpc.facade_write_block,
                    int(store_node_id), req.SerializeToString()
                )
            except Exception:
                wire = b""
        if not wire:
            it.payload = bytes(payload)
            wire = _write_block_retry(
                self.endpoint, req.SerializeToString(), self.timeout_ms
            )
        rsp.ParseFromString(wire)
        if not rsp.HasField("result"):
            return ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "empty response")
        return _result_meta_to_item_result(rsp.result.result)

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
        req = _kvdata.ReadBlockRequest()
        req.meta.request_id = self._mk_request_id("read")
        it = req.item
        it.block_hash = block_hash
        it.pool_offset = pool_offset
        it.block_size = block_size
        it.expected_store_epoch = expected_store_epoch
        it.expected_version = expected_version
        rsp = _kvdata.ReadBlockResponse()
        wire = b""
        split_payload = b""
        if self._use_facade_registry and hasattr(falconfs_kv_brpc, "facade_read_block_split"):
            try:
                wire, split_payload = _facade_rpc_retry(
                    falconfs_kv_brpc.facade_read_block_split,
                    int(store_node_id), req.SerializeToString()
                )
            except Exception:
                wire = b""
                split_payload = b""
        if self._use_facade_registry and not wire:
            try:
                wire = _facade_rpc_retry(
                    falconfs_kv_brpc.facade_read_block,
                    int(store_node_id), req.SerializeToString()
                )
            except Exception:
                wire = b""
        if not wire:
            wire = _read_block_retry(
                self.endpoint, req.SerializeToString(), self.timeout_ms
            )
        rsp.ParseFromString(wire)
        if not rsp.HasField("result"):
            return ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "empty response"), b""
        r = rsp.result
        return _result_meta_to_item_result(r.result), split_payload or r.payload

    def batch_write_blocks(
        self,
        store_node_id: int,
        blocks: Sequence[StoreBlockWrite],
    ) -> List[ItemResult]:
        if not blocks:
            return []
        req = _kvdata.BatchWriteBlockRequest()
        req.meta.request_id = self._mk_request_id("batch_write")
        payloads = []
        for b in blocks:
            it = req.items.add()
            it.block_hash = b.block_hash
            it.pool_offset = b.pool_offset
            it.block_size = b.block_size
            it.expected_store_epoch = b.expected_store_epoch
            it.expected_version = b.expected_version
            payloads.append(b.payload)
        rsp = _kvdata.BatchWriteBlockResponse()
        wire = b""
        if self._use_facade_registry:
            try:
                if hasattr(falconfs_kv_brpc, "facade_batch_write_block_payloads"):
                    wire = _facade_rpc_retry(
                        falconfs_kv_brpc.facade_batch_write_block_payloads,
                        int(store_node_id), req.SerializeToString(), payloads
                    )
                else:
                    for it, payload in zip(req.items, payloads):
                        it.payload = bytes(payload)
                    wire = _facade_rpc_retry(
                        falconfs_kv_brpc.facade_batch_write_block,
                        int(store_node_id), req.SerializeToString()
                    )
            except Exception:
                wire = b""
        if not wire:
            for it, payload in zip(req.items, payloads):
                if not it.payload:
                    it.payload = bytes(payload)
            wire = _batch_write_block_retry(
                self.endpoint, req.SerializeToString(), self.timeout_ms
            )
        rsp.ParseFromString(wire)
        out: List[ItemResult] = []
        for r in rsp.results:
            out.append(_result_meta_to_item_result(r.result))
        return out

    def batch_read_payload_views(
        self,
        store_node_id: int,
        pool_offsets: Sequence[int],
        store_epochs: Sequence[int],
        block_hashes: Sequence[bytes],
        block_size: int,
    ) -> Tuple[List[bool], List[Any], List[str]]:
        if not pool_offsets:
            return [], [], []
        if (
            self._use_facade_registry
            and hasattr(falconfs_kv_brpc, "facade_batch_read_payload_views")
        ):
            return _facade_rpc_retry(
                falconfs_kv_brpc.facade_batch_read_payload_views,
                int(store_node_id),
                list(pool_offsets),
                list(store_epochs),
                list(block_hashes),
                int(block_size),
            )
        ok, payloads = self.batch_read_payloads_fast(
            store_node_id, pool_offsets, store_epochs, block_hashes, block_size
        )
        return ok, payloads, ["bytes_fallback"] * len(payloads)

    def batch_read_payloads_fast(
        self,
        store_node_id: int,
        pool_offsets: Sequence[int],
        store_epochs: Sequence[int],
        block_hashes: Sequence[bytes],
        block_size: int,
    ) -> Tuple[List[bool], List[bytes]]:
        if not pool_offsets:
            return [], []
        if (
            self._use_facade_registry
            and hasattr(falconfs_kv_brpc, "facade_batch_read_payloads_fast")
        ):
            return _facade_rpc_retry(
                falconfs_kv_brpc.facade_batch_read_payloads_fast,
                int(store_node_id),
                list(pool_offsets),
                list(store_epochs),
                list(block_hashes),
                int(block_size),
            )
        blocks = [
            StoreBlockRead(
                pool_offset=int(pool_offset),
                block_hash=bytes(block_hash),
                block_size=int(block_size),
                expected_store_epoch=int(store_epoch),
                expected_version=0,
            )
            for pool_offset, store_epoch, block_hash in zip(pool_offsets, store_epochs, block_hashes)
        ]
        results = self.batch_read_blocks(store_node_id, blocks)
        ok_flags: List[bool] = []
        payloads: List[bytes] = []
        for result, payload in results:
            ok_flags.append(result.success)
            payloads.append(payload if result.success else b"")
        return ok_flags, payloads

    def batch_read_blocks(
        self,
        store_node_id: int,
        blocks: Sequence[StoreBlockRead],
    ) -> List[Tuple[ItemResult, bytes]]:
        if not blocks:
            return []
        req = _kvdata.BatchReadBlockRequest()
        req.meta.request_id = self._mk_request_id("batch_read")
        for b in blocks:
            it = req.items.add()
            it.block_hash = b.block_hash
            it.pool_offset = b.pool_offset
            it.block_size = b.block_size
            it.expected_store_epoch = b.expected_store_epoch
            it.expected_version = b.expected_version
        req_wire = req.SerializeToString()
        if self._use_facade_registry and hasattr(falconfs_kv_brpc, "facade_batch_read_block_fast"):
            try:
                ok_flags, payloads = _facade_rpc_retry(
                    falconfs_kv_brpc.facade_batch_read_block_fast,
                    int(store_node_id), req_wire
                )
                if len(ok_flags) == len(blocks) and len(payloads) == len(blocks):
                    out: List[Tuple[ItemResult, bytes]] = []
                    for ok, payload in zip(ok_flags, payloads):
                        if ok:
                            out.append((ItemResult(True, ErrorCode.OK, False, ""), payload))
                        else:
                            out.append((ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "batch read failed"), b""))
                    return out
            except Exception:
                pass

        rsp = _kvdata.BatchReadBlockResponse()
        wire = b""
        split_payloads = None
        if self._use_facade_registry:
            try:
                if hasattr(falconfs_kv_brpc, "facade_batch_read_block_split"):
                    wire, split_payloads = _facade_rpc_retry(
                        falconfs_kv_brpc.facade_batch_read_block_split,
                        int(store_node_id), req_wire
                    )
                else:
                    wire = _facade_rpc_retry(
                        falconfs_kv_brpc.facade_batch_read_block,
                        int(store_node_id), req_wire
                    )
            except Exception:
                wire = b""
                split_payloads = None
        if not wire:
            wire = _batch_read_block_retry(
                self.endpoint, req_wire, self.timeout_ms
            )
        rsp.ParseFromString(wire)
        out: List[Tuple[ItemResult, bytes]] = []
        for i, r in enumerate(rsp.results):
            payload = r.payload
            if split_payloads is not None and i < len(split_payloads):
                payload = split_payloads[i]
            out.append((_result_meta_to_item_result(r.result), payload))
        return out

    def read_from_ssd(
        self,
        store_node_id: int,
        evicted_path: str,
        expected_store_epoch: int,
        expected_version: int = 0,
        block_size: int = 65536,
    ) -> Tuple[ItemResult, bytes]:
        """Read spilled KV bytes via ``KVDataService.BatchReadFromSSD`` (always through the Store).

        ``store_node_id`` selects the facade when ``use_facade_registry`` is enabled
        (same pattern as :meth:`read` / :meth:`batch_read_blocks`). ``block_size`` is
        kept for API compatibility; the wire ``SSDReadItem`` only carries
        ``block_hash``, ``evicted_path``, and ``expected_version`` today.
        """
        req = _kvdata.ReadFromSSDRequest()
        req.meta.request_id = self._mk_request_id("read_ssd")
        it = req.item
        it.block_hash = b""
        it.evicted_path = evicted_path
        it.expected_version = expected_version
        rsp = _kvdata.ReadFromSSDResponse()
        wire = b""
        split_payload = b""
        if self._use_facade_registry and hasattr(falconfs_kv_brpc, "facade_read_from_ssd_split"):
            try:
                wire, split_payload = _facade_rpc_retry(
                    falconfs_kv_brpc.facade_read_from_ssd_split,
                    int(store_node_id), req.SerializeToString()
                )
            except Exception:
                wire = b""
                split_payload = b""
        if self._use_facade_registry and not wire:
            try:
                wire = _facade_rpc_retry(
                    falconfs_kv_brpc.facade_read_from_ssd,
                    int(store_node_id), req.SerializeToString()
                )
            except Exception:
                wire = b""
        if not wire:
            wire = _read_from_ssd_retry(
                self.endpoint, req.SerializeToString(), self.timeout_ms
            )
        rsp.ParseFromString(wire)
        if not rsp.HasField("result"):
            return ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "empty response"), b""
        r = rsp.result
        return _result_meta_to_item_result(r.result), split_payload or r.payload


class BrpcCluster:
    """Pair of (BrpcMetadataService, BrpcKVStore) addressed by one DN endpoint.
    Drop-in for `reference.ReferenceCluster` from the OffloadingManager's view.
    """

    def __init__(self, endpoint: str, *, dn_id: int = 1, client_id: int = 0,
                 timeout_ms: int = 30000, block_size: int = 65536,
                 store_endpoint: str | None = None, use_facade_registry: bool = False):
        from .dn_client import BrpcMetadataService
        self.endpoint = endpoint
        store_ep = store_endpoint or os.environ.get("FALCON_KV_STORE_BRPC_ENDPOINT") or endpoint
        self.metadata = BrpcMetadataService(
            endpoint, dn_id=dn_id, client_id=client_id,
            timeout_ms=timeout_ms, block_size=block_size,
        )
        self.store = BrpcKVStore(
            store_ep,
            timeout_ms=timeout_ms,
            use_facade_registry=use_facade_registry,
        )
