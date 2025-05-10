import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import _pyfalconfs_internal
from functools import wraps

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