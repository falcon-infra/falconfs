#!/bin/bash

set -euo pipefail

TESTFILE=$1/file
df -T | grep -q 'fuse.falcon_client' || {
    echo "Error: falcon_client not mounted!" >&2
    exit 1
}
# 1. 大文件
echo "=========RW tests for large file, filename: $TESTFILE!========="
# 1.1 bio测试
echo "=========BIO Tests!========="
# 1.1.1 顺序写
fio --filename=$TESTFILE --iodepth=32 --ioengine=libaio --direct=0 --unlink=0 --rw=write --bs=4k --size=20M --numjobs=4 --runtime=10 --group_reporting --name=test-write
# 1.1.2 覆盖写
fio --filename=$TESTFILE --iodepth=32 --ioengine=libaio --overwrite=1 --direct=0 --unlink=1 --rw=write --bs=4k --size=20M --numjobs=4 --runtime=10 --group_reporting --name=test-overwrite
# 1.1.3 顺序读
fio --filename=$TESTFILE --iodepth=32 --ioengine=libaio --direct=0 --unlink=1 --rw=read --bs=4k --size=20M --numjobs=4 --runtime=10 --group_reporting --name=test-read
# 1.1.4 随机写
fio --filename=$TESTFILE --iodepth=32 --ioengine=libaio --direct=0 --unlink=1 --rw=randwrite --bs=4k --size=20M --numjobs=4 --runtime=10 --group_reporting --name=test-rand-write
# 1.1.5 随机读
fio --filename=$TESTFILE -iodepth=64 --ioengine=libaio --direct=0 --unlink=1 --rw=randread --bs=4k --size=20M --numjobs=4 --runtime=10 --group_reporting --name=test-rand-read
# 1.1.6 随机读写
fio --filename=$TESTFILE --iodepth=64 --ioengine=libaio --direct=0 --unlink=1 --rw=randrw --bs=4k --size=20M --numjobs=4 --runtime=10 --group_reporting --name=test-rand-readwrite

# 1.2 dio测试
echo "=========DIO Tests!========="
# 1.2.1 顺序写
fio --filename=$TESTFILE --iodepth=32 --ioengine=libaio --direct=1 --unlink=0 --rw=write --bs=4k --size=20M --numjobs=4 --runtime=10 --group_reporting --name=test-write
# 1.2.2 覆盖写
fio --filename=$TESTFILE --iodepth=32 --ioengine=libaio --overwrite=1 --direct=1 --unlink=1 --rw=write --bs=4k --size=20M --numjobs=4 --runtime=10 --group_reporting --name=test-overwrite
# 1.2.3 顺序读
fio --filename=$TESTFILE --iodepth=32 --ioengine=libaio --direct=1 --unlink=1 --rw=read --bs=4k --size=20M --numjobs=4 --runtime=10 --group_reporting --name=test-read
# 1.2.4 随机写
fio --filename=$TESTFILE --iodepth=32 --ioengine=libaio --direct=1 --unlink=1 --rw=randwrite --bs=4k --size=20M --numjobs=4 --runtime=10 --group_reporting --name=test-rand-write
# 1.2.5 随机读
fio --filename=$TESTFILE -iodepth=64 --ioengine=libaio --direct=1 --unlink=1 --rw=randread --bs=4k --size=20M --numjobs=4 --runtime=10 --group_reporting --name=test-rand-read
# 1.2.6 随机读写
fio --filename=$TESTFILE --iodepth=64 --ioengine=libaio --direct=1 --unlink=1 --rw=randrw --bs=4k --size=20M --numjobs=4 --runtime=10 --group_reporting --name=test-rand-readwrite

# 2. 小文件
echo "=========RW tests for small file!========="
# 2.1 bio测试
echo "=========BIO Tests!========="
# 2.1.1 顺序写
fio --directory=$1 --iodepth=32 --ioengine=libaio --direct=0 --unlink=0 --rw=write --bs=4k --size=1M --numjobs=4 --runtime=10 --group_reporting --name=test-write
# 2.1.2 覆盖写
fio --directory=$1 --iodepth=32 --ioengine=libaio --overwrite=1 --direct=0 --unlink=1 --rw=write --bs=4k --size=1M --numjobs=4 --runtime=10 --group_reporting --name=test-overwrite
# 2.1.3 顺序读
fio --directory=$1 --iodepth=32 --ioengine=libaio --direct=0 --unlink=1 --rw=read --bs=4k --size=1M --numjobs=4 --runtime=10 --group_reporting --name=test-read
# 2.1.4 随机写
fio --directory=$1 --iodepth=32 --ioengine=libaio --direct=0 --unlink=1 --rw=randwrite --bs=4k --size=1M --numjobs=4 --runtime=10 --group_reporting --name=test-rand-write
# 2.1.5 随机读
fio --directory=$1 -iodepth=64 --ioengine=libaio --direct=0 --unlink=1 --rw=randread --bs=4k --size=1M --numjobs=4 --runtime=10 --group_reporting --name=test-rand-read
# 2.1.6 随机读写
fio --directory=$1 --iodepth=64 --ioengine=libaio --direct=0 --unlink=1 --rw=randrw --bs=4k --size=1M --numjobs=4 --runtime=10 --group_reporting --name=test-rand-readwrite

# 2.2 dio测试
echo "=========DIO Tests!========="
# 2.2.1 顺序写
fio --directory=$1 --iodepth=32 --ioengine=libaio --direct=1 --unlink=0 --rw=write --bs=4k --size=1M --numjobs=4 --runtime=10 --group_reporting --name=test-write
# 2.2.2 覆盖写
fio --directory=$1 --iodepth=32 --ioengine=libaio --overwrite=1 --direct=1 --unlink=1 --rw=write --bs=4k --size=1M --numjobs=4 --runtime=10 --group_reporting --name=test-overwrite
# 2.2.3 顺序读
fio --directory=$1 --iodepth=32 --ioengine=libaio --direct=1 --unlink=1 --rw=read --bs=4k --size=1M --numjobs=4 --runtime=10 --group_reporting --name=test-read
# 2.2.4 随机写
fio --directory=$1 --iodepth=32 --ioengine=libaio --direct=1 --unlink=1 --rw=randwrite --bs=4k --size=1M --numjobs=4 --runtime=10 --group_reporting --name=test-rand-write
# 2.2.5 随机读
fio --directory=$1 -iodepth=64 --ioengine=libaio --direct=1 --unlink=1 --rw=randread --bs=4k --size=1M --numjobs=4 --runtime=10 --group_reporting --name=test-rand-read
# 2.2.6 随机读写
fio --directory=$1 --iodepth=64 --ioengine=libaio --direct=1 --unlink=1 --rw=randrw --bs=4k --size=1M --numjobs=4 --runtime=10 --group_reporting --name=test-rand-readwrite
