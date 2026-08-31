#!/usr/bin/env python3
"""v6.6.2: BrpcKVStore.read_from_ssd uses KVDataService only (facade + BRPC), store_node_id first."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv import kv_data_service_pb2 as _kvdata  # noqa: E402
from falconfs_kv.store_client import BrpcKVStore  # noqa: E402


class TestStoreClientReadFromSsd(unittest.TestCase):
    def test_read_from_ssd_fallback_uses_batch_brpc(self) -> None:
        store = BrpcKVStore("127.0.0.1:9", use_facade_registry=False)
        ok_rsp = _kvdata.ReadFromSSDResponse()
        ok_rsp.result.result.success = True
        ok_rsp.result.payload = b"ssd-bytes"

        with mock.patch("falconfs_kv.store_client.falconfs_kv_brpc") as mbrpc:
            mbrpc.read_from_ssd.return_value = ok_rsp.SerializeToString()
            ir, payload = store.read_from_ssd(
                3,
                "/tmp/falconfs/evicted/x.kv",
                expected_store_epoch=1,
                expected_version=42,
            )
        self.assertTrue(ir.success)
        self.assertEqual(payload, b"ssd-bytes")
        mbrpc.read_from_ssd.assert_called_once()
        mbrpc.facade_batch_read_from_ssd.assert_not_called()
        args, _kwargs = mbrpc.read_from_ssd.call_args
        self.assertEqual(args[0], "127.0.0.1:9")
        wire = args[1]
        req = _kvdata.ReadFromSSDRequest()
        req.ParseFromString(wire)
        self.assertEqual(req.item.evicted_path, "/tmp/falconfs/evicted/x.kv")
        self.assertEqual(req.item.expected_version, 42)

    def test_read_from_ssd_prefers_facade_when_enabled(self) -> None:
        store = BrpcKVStore("127.0.0.1:9", use_facade_registry=True)
        ok_rsp = _kvdata.ReadFromSSDResponse()
        ok_rsp.result.result.success = True
        ok_rsp.result.payload = b"via-facade"

        with mock.patch("falconfs_kv.store_client.falconfs_kv_brpc") as mbrpc:
            mbrpc.facade_read_from_ssd.return_value = ok_rsp.SerializeToString()
            ir, payload = store.read_from_ssd(
                5,
                "/ssd/a.kv",
                expected_store_epoch=2,
                expected_version=0,
            )
        self.assertTrue(ir.success)
        self.assertEqual(payload, b"via-facade")
        mbrpc.facade_read_from_ssd.assert_called_once_with(
            5, mock.ANY
        )
        mbrpc.read_from_ssd.assert_not_called()

    def test_read_from_ssd_facade_exception_falls_back_to_brpc(self) -> None:
        store = BrpcKVStore("127.0.0.1:9", use_facade_registry=True)
        ok_rsp = _kvdata.ReadFromSSDResponse()
        ok_rsp.result.result.success = True
        ok_rsp.result.payload = b"fallback"

        with mock.patch("falconfs_kv.store_client.falconfs_kv_brpc") as mbrpc:
            mbrpc.facade_read_from_ssd.side_effect = RuntimeError("no registry")
            mbrpc.read_from_ssd.return_value = ok_rsp.SerializeToString()
            ir, payload = store.read_from_ssd(1, "/p", 0)
        self.assertTrue(ir.success)
        self.assertEqual(payload, b"fallback")
        mbrpc.read_from_ssd.assert_called_once()


if __name__ == "__main__":
    unittest.main()
