# FalconFS

[![Build](https://github.com/falcon-infra/falconfs/actions/workflows/build.yml/badge.svg)](https://github.com/falcon-infra/falconfs/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/License-Mulan%20PSL%202-green)](LICENSE)

FalconFS is a high-performance distributed file system (DFS) optimized for AI workloads. It addresses the following challenges:  

1. **Massive small files** – Its high-performance distributed metadata engine dramatically improves I/O throughput of handling massive small files (e.g., images), eliminating storage bottlenecks in AI data preprocessing and model training.

2. **High throughput requirement** – In tiered storage (i.e., DRAM, SSD and elastic object store), FalconFS can aggregates near-compute DRAM and SSDs to provide over TB/s high throughput for AI workloads (e.g., model training, data preprocessing and KV cache offloading).

3. **Large scale** - FalconFS can scale to thousands of NPUs through its scale-out metadata engine and scale-up single metadata performance.

Through the above advantages, FalconFS delivers an ideal storage solution for modern AI workloads and has been running in Huawei autonomous driving system's production environment with 10,000 NPUs.

## Documents

- [FalconFS Design](./docs/design.md)
- [FalconFS Cluster Test Setup Guide](./docs/setup.md)

## Architecture
![FalconFS Architecture](https://github.com/user-attachments/assets/5ff1e80a-4cce-4b05-a35d-8da54191fb30)

## Performance

**Test Environment Configuration:**
- **CPU:** 2 x Intel Xeon 3.00GHz, 12 cores
- **Memory:** 16 x DDR4 2933 MHz 16GB
- **Storage:** 2 x NVMe SSD
- **Network:** 2 x 100GbE
- **OS:** Ubuntu 20.04 Server 64-bit

> **ℹ️ Note**  
> This experiment uses an optimized Linux fuse module. The relevant code will be open-sourced in the near future.

We conduct the experiments in a cluster of 13 dual-socket machines, whose configuration is shown above. To better simulate large scale deployment in data centers, we have the following setups:
- First, to expand the test scale, we abstract each machine into two nodes, with each node bound to one socket, one SSD, and one NIC, scaling up the testbed to 26 nodes.
- Second, to simulate the resource ratio in real deployment, we reduce the server resources to 4 cores per node. So that we can:
  - generate sufficient load to stress the servers with a few client nodes.
  - correctly simulate the 4:1 ratio between CPU cores and NVMe SSDs in typical real deployments.
In the experiments below, we run 4 metadata nodes and 12 data nodes for each DFS instance and saturate them with 10 client nodes. All DFSs do not enable metadata or data replication.

**Compared Systems:**
- CephFS 12.2.13.
- JuiceFS 1.2.1, with TiKV 1.16.1 as the metadata engine and data store.
- Lustre 2.15.6.


<br>
<div style="text-align: center;">
    <font size="5">
        <b>Throughput of File data IO.</b>
    </font>
    <br>We evaluate the performance of accessing small files with different file sizes. As shown in following figures, Y-axis is the throughput normalized to that of FalconFS. Thanks to FalconFS's higher metadata performance, it outperforms other DFSs in small file access. For files no larger than 64 KB, FalconFS achieves 7.35--21.23x speedup over CephFS, 0.86--24.87x speedup over JuiceFS and 1.12--1.85x speedup over Lustre. For files whose size is larger than 256 KiB, the performance of FalconFS is bounded by the aggregated SSD bandwidth. 
</div>

![read-throughput](https://github.com/user-attachments/assets/c22f1e42-5a55-4f82-b08c-908cdc8aca4d)
![write-throughput](https://github.com/user-attachments/assets/73f73b19-1664-4c72-a712-592afbc931d6)
<br>

<div style="text-align: center;">
    <font size="5">
        <b>MLPerf ResNet-50 Training Storage Benchmark.</b>
    </font>
    <br> We simulate training ResNet-50 model on a dataset containing 10 million files, each file contains one 131 KB object, which is a typical scenario for deep learning model training in production. MLPerf has been modified to avoid merging small files into large ones, simulating real-world business scenarios while reducing the overhead associated with merge and copy operations. Taking 90% accelerator utilization as the threshold, FalconFS supports up to 80 accelerators while Lustre can only support 32 accelerators on the experiment hardware.
</div>

![mlperf](https://github.com/user-attachments/assets/30f4e24f-a933-49b8-8163-306b1c45e3c0)

<br>

## Build

suppose at the `~/code` dir
``` bash
git clone https://github.com/falcon-infra/falconfs.git
cd falconfs
git submodule update --init --recursive # submodule update postresql
./patches/apply.sh
docker run -it --rm -v `pwd`/..:/root/code -w /root/code/falconfs ghcr.io/falcon-infra/falconfs-dev:0.1.1 /bin/zsh
./build.sh
ln -s /root/code/falconfs/falcon/build/compile_commands.json . # use for clangd
```

test

``` bash
./build.sh test
```

clean

``` bash
cd falconfs
./build.sh clean
```

incermental build and clean

``` bash
cd falconfs
./build.sh build pg # only build pg
./build.sh clean pg # only clean pg
./build.sh build falcon # only build falconfs
./build.sh clean falcon # only clean falconfs
./build.sh build falcon --debug # build falconfs with debug
```
## Copyright
Copyright (c) 2025 Huawei Technologies Co., Ltd.
