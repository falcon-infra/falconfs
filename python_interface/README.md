# Install

1. Build and install this project first according to ${PROJECT_SOURCE_DIR}/README.md. For example:
```
cd ${PROJECT_SOURCE_DIR}
./build.sh
./build.sh install
```

2. Run `pip install .` under ${PROJECT_SOURCE_DIR}/python_interface.

# Usage Example
```
import pyfalconfs

KvClient = pyfalconfs.KvClient("/home/falconfs/py_workspace", "/home/falconfs/code/falconfs/config/config.json")
buffer = bytearray(b"hello")
ret = KvClient.Put("kvcache", buffer)
assert(ret == 0)

valueBuffer = bytearray(20)
ret = KvClient.Get("kvcache", valueBuffer)
assert(ret == 0)
print(valueBuffer)

ret = KvClient.Delete("kvcache")
assert(ret == 0)

```

# NOTICE

- Don't create more than one Client at the same time.