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

client = pyfalconfs.Client("/home/falconfs/py_workspace", "/home/falconfs/code/falconfs/config/config.json")

ret, fd = client.Create("/test", 2)
assert(ret == 0)

buffer = bytearray(b"hello")
ret = client.Write("/test", fd, buffer, 5, 0)
assert(ret == 0)

anotherBuffer = bytearray(16)
ret = client.Read("/test", fd, anotherBuffer, 5, 0)
assert(ret == 5)

ret = client.Flush("/test", fd)
assert(ret == 0)

ret = client.Close("/test", fd)
assert(ret == 0)

print(anotherBuffer)
```

# TODO

- Deconstruct. Notice: Currently a client cannot be deconstructed, so never try to release it. It can only be deconstructed as the program exited.

# NOTICE

- Don't create more than one Client at the same time.