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

class KvClient:
    @copy_doc_from(_pyfalconfs_internal.KvInit)
    def __init__(self, workspace, running_config_file):
        _pyfalconfs_internal.KvInit(workspace, running_config_file)

    @copy_doc_from(_pyfalconfs_internal.KvPut)
    def Put(self, key, value):
        return _pyfalconfs_internal.KvPut(key, value)

    @copy_doc_from(_pyfalconfs_internal.KvGet)
    def Get(self, key, value):
        return _pyfalconfs_internal.KvGet(key, value)

    @copy_doc_from(_pyfalconfs_internal.KvDelete)
    def Delete(self, key):
        return _pyfalconfs_internal.KvDelete(key)