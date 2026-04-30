import errno
import importlib
import os
import tempfile
import unittest


class PyFalconFSInternalCoverageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = importlib.import_module("_pyfalconfs_internal")

    def test_init_reports_missing_config_file(self):
        with tempfile.TemporaryDirectory() as workspace:
            with self.assertRaises(RuntimeError):
                self.mod.Init(workspace, os.path.join(workspace, "missing.json"))

    def test_argument_validation_errors(self):
        with self.assertRaises(TypeError):
            self.mod.Mkdir()
        with self.assertRaises(TypeError):
            self.mod.Create("/file")
        with self.assertRaises(TypeError):
            self.mod.Read("/file", 1, bytearray(1), "bad", 0)
        with self.assertRaises(TypeError):
            self.mod.Write("/file", 1, bytearray(1), "bad", 0)

    def test_buffer_size_validation_errors(self):
        with self.assertRaises(RuntimeError):
            self.mod.Read("/file", 1, bytearray(2), 3, 0)
        with self.assertRaises(RuntimeError):
            self.mod.Write("/file", 1, bytearray(2), 3, 0)

    def test_fast_paths_that_do_not_require_service(self):
        ret, stbuf = self.mod.Stat("/\x011synthetic")
        self.assertEqual(ret, 0)
        self.assertEqual(stbuf["st_mode"], 0o40777)

        ret, fd = self.mod.OpenDir("")
        self.assertEqual(ret, -errno.EINVAL)
        self.assertEqual(fd, 0)

        self.assertEqual(self.mod.Close("/missing", 123456789), -errno.EBADF)
        self.assertEqual(self.mod.Flush("/missing", 123456789), -errno.EBADF)
        self.assertEqual(self.mod.CloseDir("/missing", 123456789), -errno.EBADF)


if __name__ == "__main__":
    unittest.main()
