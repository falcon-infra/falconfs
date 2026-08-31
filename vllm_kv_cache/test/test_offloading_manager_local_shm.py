#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

try:
    from falconfs_kv import kv_common_pb2 as _kvcommon  # noqa: E402
    from falconfs_kv import kv_data_service_pb2 as _kvdata  # noqa: E402
    from falconfs_kv.store_client import BrpcKVStore  # noqa: E402
    _HAS_EXT = True
except Exception:
    _HAS_EXT = False


def _ok_write_wire() -> bytes:
    rsp = _kvdata.WriteBlockResponse()
    rsp.result.result.success = True
    rsp.result.result.error_code = _kvcommon.OK
    return rsp.SerializeToString()


def _ok_read_wire(payload: bytes) -> bytes:
    rsp = _kvdata.ReadBlockResponse()
    rsp.result.result.success = True
    rsp.result.result.error_code = _kvcommon.OK
    rsp.result.payload = payload
    return rsp.SerializeToString()


@unittest.skipUnless(_HAS_EXT, "falconfs_kv_brpc extension not available")
class OffloadingManagerLocalShmTest(unittest.TestCase):
    def test_facade_write_path_used_when_enabled(self):
        store = BrpcKVStore("127.0.0.1:18765", timeout_ms=2000, use_facade_registry=True)
        with mock.patch(
            "falconfs_kv.store_client.falconfs_kv_brpc.facade_write_block_payload",
            return_value=_ok_write_wire(),
        ) as p_facade_write, mock.patch(
            "falconfs_kv.store_client.falconfs_kv_brpc.facade_write_block",
            side_effect=AssertionError("legacy serialized facade write should not be used"),
        ), mock.patch(
            "falconfs_kv.store_client.falconfs_kv_brpc.write_block",
            side_effect=AssertionError("network write should not be used"),
        ):
            result = store.write(1, 0, b"abc", 1, 4096)
            self.assertTrue(result.success)
            p_facade_write.assert_called_once()

    def test_facade_read_path_used_when_enabled(self):
        store = BrpcKVStore("127.0.0.1:18765", timeout_ms=2000, use_facade_registry=True)
        with mock.patch(
            "falconfs_kv.store_client.falconfs_kv_brpc.facade_read_block_split",
            return_value=(_ok_read_wire(b""), b"payload"),
        ) as p_facade_read, mock.patch(
            "falconfs_kv.store_client.falconfs_kv_brpc.facade_read_block",
            side_effect=AssertionError("legacy serialized facade read should not be used"),
        ), mock.patch(
            "falconfs_kv.store_client.falconfs_kv_brpc.read_block",
            side_effect=AssertionError("network read should not be used"),
        ):
            result, payload = store.read(1, 0, 1)
            self.assertTrue(result.success)
            self.assertEqual(payload, b"payload")
            p_facade_read.assert_called_once()


if __name__ == "__main__":
    unittest.main()
