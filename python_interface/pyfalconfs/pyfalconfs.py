import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import _pyfalconfs_internal
from collections import namedtuple
from functools import wraps

# Result type for async operations carrying C++ timing information.
# Fields:
#   value (int):           The operation return value.
#   cpp_recv_time_us (int): C++ worker thread receive timestamp (steady_clock, us).
#   cpp_done_time_us (int): C++ worker thread done timestamp (steady_clock, us).
AsyncOpResult = namedtuple(
    "AsyncOpResult", ["value", "cpp_recv_time_us", "cpp_done_time_us"]
)

# Result type for sync operations carrying C++ timing information.
# Fields:
#   ret (int):              Operation return code (0 = success).
#   stat_dict (dict):       File status dictionary (st_size, st_mode, etc.).
#   cpp_entry_time_us (int): C++ function entry timestamp (steady_clock, us).
#   cpp_exit_time_us (int):  C++ function exit timestamp (steady_clock, us).
SyncOpResult = namedtuple(
    "SyncOpResult",
    ["ret", "stat_dict", "cpp_entry_time_us", "cpp_exit_time_us"],
)

def copy_doc_from(source_method):
    def decorator(target_method):
        target_method.__doc__ = source_method.__doc__
        return target_method
    return decorator

class Client:
    @copy_doc_from(_pyfalconfs_internal.Init)
    def __init__(self, workspace, running_config_file):
        _pyfalconfs_internal.Init(workspace, running_config_file)

    def __del__(self):
        pass

    @copy_doc_from(_pyfalconfs_internal.Mkdir)
    def Mkdir(self, path):
        return _pyfalconfs_internal.Mkdir(path)

    @copy_doc_from(_pyfalconfs_internal.Rmdir)
    def Rmdir(self, path):
        return _pyfalconfs_internal.Rmdir(path)

    @copy_doc_from(_pyfalconfs_internal.Create)
    def Create(self, path, oflags):
        return _pyfalconfs_internal.Create(path, oflags)

    @copy_doc_from(_pyfalconfs_internal.Unlink)
    def Unlink(self, path):
        return _pyfalconfs_internal.Unlink(path)

    @copy_doc_from(_pyfalconfs_internal.Open)
    def Open(self, path, oflags):
        return _pyfalconfs_internal.Open(path, oflags)

    @copy_doc_from(_pyfalconfs_internal.Flush)
    def Flush(self, path, fd):
        return _pyfalconfs_internal.Flush(path, fd)

    @copy_doc_from(_pyfalconfs_internal.Close)
    def Close(self, path, fd):
        return _pyfalconfs_internal.Close(path, fd)

    @copy_doc_from(_pyfalconfs_internal.Read)
    def Read(self, path, fd, buffer, size, offset):
        return _pyfalconfs_internal.Read(path, fd, buffer, size, offset)

    @copy_doc_from(_pyfalconfs_internal.Write)
    def Write(self, path, fd, buffer, size, offset):
        return _pyfalconfs_internal.Write(path, fd, buffer, size, offset)

    @copy_doc_from(_pyfalconfs_internal.Stat)
    def Stat(self, path):
        result = _pyfalconfs_internal.Stat(path)
        # New C++ returns 4-tuple: (ret, dict, entry_us, exit_us)
        if isinstance(result, tuple) and len(result) == 4:
            return SyncOpResult(*result)
        # Old C++ returns 2-tuple: (ret, dict) — fill timing with 0
        return SyncOpResult(result[0], result[1], 0, 0)

    @copy_doc_from(_pyfalconfs_internal.OpenDir)
    def OpenDir(self, path):
        return _pyfalconfs_internal.OpenDir(path)

    @copy_doc_from(_pyfalconfs_internal.CloseDir)
    def CloseDir(self, path, fd):
        return _pyfalconfs_internal.CloseDir(path, fd)

    @copy_doc_from(_pyfalconfs_internal.ReadDir)
    def ReadDir(self, path, fd):
        return _pyfalconfs_internal.ReadDir(path, fd)

    # ==================== Timing Helper ====================

    def GetSteadyClockUs(self):
        """Get current C++ steady_clock timestamp in microseconds."""
        return _pyfalconfs_internal.GetSteadyClockUs()

    # ==================== 异步方法 ====================

    @copy_doc_from(_pyfalconfs_internal.AsyncExists)
    async def AsyncExists(self, path):
        state = _pyfalconfs_internal.AsyncExists(path)
        result = await state
        return AsyncOpResult(
            value=result,
            cpp_recv_time_us=state.cpp_recv_time_us,
            cpp_done_time_us=state.cpp_done_time_us,
        )

    @copy_doc_from(_pyfalconfs_internal.AsyncGet)
    async def AsyncGet(self, path, buffer, size, offset):
        state = _pyfalconfs_internal.AsyncGet(path, buffer, size, offset)
        result = await state
        return AsyncOpResult(
            value=result,
            cpp_recv_time_us=state.cpp_recv_time_us,
            cpp_done_time_us=state.cpp_done_time_us,
        )

    @copy_doc_from(_pyfalconfs_internal.AsyncPut)
    async def AsyncPut(self, path, buffer, size, offset):
        state = _pyfalconfs_internal.AsyncPut(path, buffer, size, offset)
        result = await state
        return AsyncOpResult(
            value=result,
            cpp_recv_time_us=state.cpp_recv_time_us,
            cpp_done_time_us=state.cpp_done_time_us,
        )
