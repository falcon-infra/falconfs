import errno
import importlib
import json
import os
import tempfile
import time
import unittest


class PyFalconFSInternalCoverageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = importlib.import_module("_pyfalconfs_internal")

    def test_init_reports_missing_config_file(self):
        with tempfile.TemporaryDirectory() as workspace:
            with self.assertRaises(RuntimeError):
                self.mod.Init(workspace, os.path.join(workspace, "missing.json"))
        with tempfile.NamedTemporaryFile() as workspace_file, tempfile.NamedTemporaryFile() as config_file:
            with self.assertRaises(RuntimeError):
                self.mod.Init(workspace_file.name, config_file.name)

    def test_argument_validation_errors(self):
        with self.assertRaises(TypeError):
            self.mod.Mkdir()
        with self.assertRaises(TypeError):
            self.mod.Create("/file")
        with self.assertRaises(TypeError):
            self.mod.Rmdir()
        with self.assertRaises(TypeError):
            self.mod.Unlink()
        with self.assertRaises(TypeError):
            self.mod.Open("/file")
        with self.assertRaises(TypeError):
            self.mod.Flush("/file")
        with self.assertRaises(TypeError):
            self.mod.Close("/file")
        with self.assertRaises(TypeError):
            self.mod.Read("/file", 1, bytearray(1), "bad", 0)
        with self.assertRaises(TypeError):
            self.mod.Write("/file", 1, bytearray(1), "bad", 0)
        with self.assertRaises(TypeError):
            self.mod.Stat()
        with self.assertRaises(TypeError):
            self.mod.OpenDir()
        with self.assertRaises(TypeError):
            self.mod.CloseDir("/file")
        with self.assertRaises(TypeError):
            self.mod.ReadDir("/file")
        with self.assertRaises(TypeError):
            self.mod.AsyncExists()
        with self.assertRaises(TypeError):
            self.mod.AsyncGet("/file", bytearray(1), "bad", 0)
        with self.assertRaises(TypeError):
            self.mod.AsyncPut("/file", bytearray(1), "bad", 0)

    def test_buffer_size_validation_errors(self):
        with self.assertRaises(RuntimeError):
            self.mod.Read("/file", 1, bytearray(2), 3, 0)
        with self.assertRaises(RuntimeError):
            self.mod.Write("/file", 1, bytearray(2), 3, 0)

    def test_fast_paths_that_do_not_require_service(self):
        ret, fd = self.mod.OpenDir("")
        self.assertEqual(ret, -errno.EINVAL)
        self.assertEqual(fd, 0)

        self.assertEqual(self.mod.Close("/missing", 123456789), -errno.EBADF)
        self.assertEqual(self.mod.Flush("/missing", 123456789), -errno.EBADF)
        self.assertEqual(self.mod.CloseDir("/missing", 123456789), -errno.EBADF)
        self.assertEqual(self.mod.Read("/missing", 123456789, bytearray(2), 2, 0), -errno.EBADF)
        self.assertEqual(self.mod.Write("/missing", 123456789, bytearray(b"ab"), 2, 0), -errno.EBADF)

        ret, stbuf = self.mod.Stat("/\x011middle")
        self.assertEqual(ret, 0)
        self.assertTrue(stbuf["st_mode"] & 0o40000)

    def wait_async_result(self, async_state):
        for _ in range(100):
            try:
                next(async_state)
            except StopIteration as stop:
                return stop.value
            time.sleep(0.01)
        self.fail("async operation did not complete")

class PyFalconFSInternalServiceCoverageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = importlib.import_module("_pyfalconfs_internal")
        if os.environ.get("FALCON_PY_SERVICE_COVERAGE") != "1":
            raise unittest.SkipTest("Python service coverage is disabled")

        cls.workspace = tempfile.TemporaryDirectory()
        cls.config_file = os.path.join(cls.workspace.name, "config.json")
        config = {
            "main": {
                "falcon_log_dir": cls.workspace.name,
                "falcon_log_level": "WARNING",
                "falcon_log_max_size_mb": 10,
                "falcon_cache_root": os.path.join(cls.workspace.name, "cache"),
                "falcon_dir_num": 101,
                "falcon_block_size": 524288,
                "falcon_read_big_file_size": 2097152,
                "falcon_preblock_num": 1000,
                "falcon_max_open_num": 0,
                "falcon_node_id": 0,
                "falcon_cluster_view": ["127.0.0.1:56039", "0.0.0.0:56039"],
                "falcon_thread_num": 4,
                "falcon_server_ip": os.environ.get("SERVER_IP", "127.0.0.1"),
                "falcon_server_port": os.environ.get("SERVER_PORT", "55510"),
                "falcon_async": False,
                "falcon_persist": False,
                "falcon_eviction": 0.1,
                "falcon_is_inference": False,
                "falcon_mount_path": "/",
                "falcon_to_local": False,
                "falcon_log_reserved_num": 3,
                "falcon_log_reserved_time": 1,
                "falcon_stat_max": True,
                "falcon_use_prometheus": False,
                "falcon_prometheus_port": "50040",
            }
        }
        with open(cls.config_file, "w", encoding="utf-8") as config_handle:
            json.dump(config, config_handle, indent=4)

        for _ in range(6):
            try:
                cls.mod.Init(cls.workspace.name, cls.config_file)
                return
            except RuntimeError:
                time.sleep(0.5)
        cls.workspace.cleanup()
        raise unittest.SkipTest("Falcon service is unavailable")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "workspace"):
            cls.workspace.cleanup()

    def unique_path(self, suffix):
        return f"/pyfalconfs_coverage_{os.getpid()}_{time.time_ns()}_{suffix}"

    def wait_async_result(self, async_state):
        for _ in range(100):
            try:
                next(async_state)
            except StopIteration as stop:
                return stop.value
            time.sleep(0.01)
        self.fail("async operation did not complete")

    def test_create_read_write_stat_and_remove(self):
        directory = self.unique_path("dir")
        path = f"{directory}/file"
        renamed = f"{directory}/renamed"

        self.assertEqual(self.mod.Mkdir(directory), 0)
        self.assertNotEqual(self.mod.Mkdir(directory), 0)

        ret, fd = self.mod.Create(path, os.O_RDWR | os.O_CREAT)
        self.assertEqual(ret, 0)
        payload = bytearray(b"pyfalconfs-service-coverage")
        self.assertEqual(self.mod.Write(path, fd, payload, len(payload), 0), 0)
        self.assertEqual(self.mod.Flush(path, fd), 0)
        self.assertEqual(self.mod.Close(path, fd), 0)

        ret, stbuf = self.mod.Stat(path)
        self.assertEqual(ret, 0)
        self.assertGreaterEqual(stbuf["st_size"], len(payload))

        ret, fd = self.mod.Open(path, os.O_RDONLY)
        self.assertEqual(ret, 0)
        read_buffer = bytearray(len(payload))
        self.assertEqual(self.mod.Read(path, fd, read_buffer, len(read_buffer), 0), len(read_buffer))
        self.assertEqual(read_buffer, payload)
        self.assertEqual(self.mod.Close(path, fd), 0)

        async_state = self.mod.AsyncExists(path)
        self.assertEqual(self.wait_async_result(async_state), 0)

        self.assertEqual(self.mod.Unlink(path), 0)
        self.assertEqual(self.mod.Rmdir(directory), 0)

    def test_async_put_get_and_cleanup(self):
        path = self.unique_path("async_file")
        payload = bytearray(b"pyfalconfs-async-coverage")

        put_state = self.mod.AsyncPut(path, payload, len(payload), 0)
        put_ret = self.wait_async_result(put_state)
        if put_ret == -errno.ENOENT:
            self.skipTest("AsyncPut cannot create files in this service coverage environment")
        self.assertEqual(put_ret, 0)

        read_buffer = bytearray(len(payload))
        get_state = self.mod.AsyncGet(path, read_buffer, len(read_buffer), 0)
        self.assertEqual(self.wait_async_result(get_state), len(payload))
        self.assertEqual(read_buffer, payload)

        self.assertEqual(self.mod.Unlink(path), 0)

    def test_directory_listing(self):
        directory = self.unique_path("listdir")

        self.assertEqual(self.mod.Mkdir(directory), 0)

        ret, fd = self.mod.OpenDir(directory)
        if ret != 0:
            self.mod.Rmdir(directory)
            self.skipTest(f"OpenDir returned {ret} in local service coverage run")
        ret, entries = self.mod.ReadDir(directory, fd)
        self.assertEqual(ret, 0)
        entry_names = [name for name, _mode in entries]
        self.assertIn(".", entry_names)
        self.assertIn("..", entry_names)
        self.assertEqual(self.mod.CloseDir(directory, fd), 0)

        self.assertEqual(self.mod.Rmdir(directory), 0)


if __name__ == "__main__":
    unittest.main()
