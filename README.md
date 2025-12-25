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
- Second, to simulate the resource ratio in real deployment, we set the server resources to 4 cores per node, so as to saturate the servers with a limited number of client nodes.
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
    <br>We evaluate the performance of accessing small files with different file sizes. As shown in following figures, Y-axis is the throughput normalized to that of FalconFS. Thanks to FalconFS's higher metadata performance, it outperforms other DFSs in small file access. For files no larger than 64 KB, FalconFS achieves 7.35--21.23x speedup over CephFS, 2.94--23.53× speedup over JuiceFS and 1.12--1.85x speedup over Lustre. For files whose size is larger than 256 KiB, the performance of FalconFS is bounded by the aggregated SSD bandwidth. 
</div>

<img width="600" height="40" alt="image" src="https://github.com/user-attachments/assets/de6dfbfb-748f-4c40-82b5-cb162a162725" />
<img width="800" height="300" alt="read" src="https://github.com/user-attachments/assets/dac6ce3b-de08-41dc-920d-1ed9ce2caf43" />
<img width="800" height="300" alt="write" src="https://github.com/user-attachments/assets/5d9847bf-51fb-4165-8574-d31a15d67c7e" />

<br>

<div style="text-align: center;">
    <font size="5">
        <b>ResNet-50 Model Training.</b>
    </font>
    <br> We use MLPerf storage benchmark to simulate training ResNet-50 model on a dataset containing 10 million files under 1 million directories, with each file sized at 112 KiB, which is a typical scenario for deep learning model training in production. Taking 90% accelerator utilization as the threshold, FalconFS supports up to 80 accelerators while Lustre can only support 32 accelerators on the experiment hardware. The MLPerf benchmark is rewritten using C++ to provide higher concurrency, allowing for saturating the servers with a limited number of client machines.
</div>

![mlperf](https://github.com/user-attachments/assets/30f4e24f-a933-49b8-8163-306b1c45e3c0)

<br>


### Metadata Performance

**Test Environment Configuration:**
- **OS container over VM:** OpenEuler 22.03-kernel 5.10.0
- **CPU:** Kunpeng 920 2.9GHz, 160 cores, arm
- **Memory:** 1536GiB
- **Storage:** 10 x 3.2TiB NVMe SSD
- **Network:** 100Gbps

We conduct a metadata performance experiment in a cluster of 5 servers, whose configuration is shown above. We deploy the FalconFS metadata engine on one single server and use the remaining four servers as clients. 
To improve NUMA locality, we start four metadata DNs each binding to one NUMA node within the metadata server using the following command: numactl --cpunodebind=${i} --localalloc pg_ctl start.
We do not enable metadata replication. To saturate the metadata server's computing capacity, clients use the LibFS interface to generate as many concurrent requests as possible. The LibFS interface and test scripts can be found under falconfs/tests/private-directory-test. 
> **Note**  
> This experiment only demonstrates the throughput of one metadata server and FalconFS can deliver scalable multi-server metadata throughput.

<img width="1000" height="800" alt="image" src="https://github.com/user-attachments/assets/4572f54b-8c28-4660-b7bf-86d9935600ba" />


## Build

suppose at the `~/code` dir
``` bash
git clone https://github.com/falcon-infra/falconfs.git
cd falconfs
git submodule update --init --recursive # submodule update postresql
docker run -it --privileged -d -v `pwd`/..:/root/code -w /root/code/falconfs ghcr.io/falcon-infra/falconfs-dev:ubuntu24.04 /bin/zsh
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
## Publication
**FalconFS: Distributed File System for Large-Scale Deep Learning Pipeline**. Jingwei Xu, Junbin Kang, Mingkai Dong, Mingyu Liu, Lu Zhang, Shaohong Guo, Ziyan Qiu, Mingzhen You, Ziyi Tian, Anqi Yu, Tianhong Ding, Xinwei Hu, and Haibo Chen. To appear in the 23rd USENIX Symposium on Networked Systems Design and Implementation (NSDI), 2026 ([preprint](https://arxiv.org/abs/2507.10367))


## Copyright
Copyright (c) 2025 Huawei Technologies Co., Ltd.
