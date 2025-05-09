# Design and implementation

FalconFS is a high-performance, parallel file system designed for AI workloads, which mainly consists of the metadata engine, file data store, client and cluster management. FalconFS can be deployed across thousands of GPU/NPU machines, leverages their local DRAM and SSDs to offer a shared storage abstraction to AI applications through POSIX API and is capable of providing over TB/s aggregated read/write throughput to hundreds of billions of files. Meanwhile, FalconFS can utilize cloud object store as backend to support cost-effective and elastic storage while maintaining high performance for hot/warm data access.

As shown in the following figure, applications can access files via our Falcon client which provides both POSIX APIs and libFS APIs. The key component of FalconFS is our novel distributed metadata engine that stores the file system namespace and file metadata (e.g., file name, size, block table).

The file store engine is responsible for storing assigned files across local DRAM, SSDs and remote cloud object store. Both the metadata engine and the file store can run within compute machines and use local DRAM and SSDs to store metadata and data without additional hardware cost.

The cluster management is responsible for managing membership of metadata nodes and file store nodes. We currently adopt Zookeeper to serve such role.

![FalconFS Architecture](https://github.com/user-attachments/assets/5ff1e80a-4cce-4b05-a35d-8da54191fb30)

## Falcon metadata engine
The Falcon metadata engine adopts a shared-nothing scale-out architecture, which consists of a set of metadata servers. The Falcon metadata engine is built on top of the PostgreSQL database as an extension, which mainly uses its table and transaction management, B-link tree index, xlog (write-ahead logging), use-defined functions and primary-secondary replication to manage metadata shards. The metadata engine distributes the file system metadata across a set of PostgreSQL instances (called metadata server or meta DB) and uses two-phase commit and locking protocol to guarantee ACID properties for file system operations. Each meta DB can be configured with several secondary DBs for high availability, which forms a replication group. We adopt the cluster manager to manage the membership for each replication group, which elects a new primary DB within the group when its old primary DB fails.

To access a file, the client would first interact with the metadata engine to resolve the file path to locate its parent directory and then to look up its file metadata. Hence, the performance of the metadata engine is critical to the overall IO performance especially for small file workloads which are typical in AI data preprocessing and training scenarios.

With a set of our proposed novel techniques, the Falcon metadata engine shows several times higher throughput than the state-of-the-art systems such as Lustre. As a result, FalconFS outperforms Lustre by more than 1x in massive small file read/write workloads.

**1. Replicated directory namespace**: 
Traditional parallel file systems usually incur either distributed file path resolution cost across multiple metadata servers or client metadata caching cost. As the number of clients can be very large (e.g., thousands of compute machines), the client-caching approach introduces cost and complexity of maintaining cache consistency at a large scale and still incurs expensive distributed file path resolution cost on cache misses. To address the above challenges, FalconFS replicates the file system namespace across all the metadata servers, such that each metadata server can resolve file path and check permissions locally. The namespace (i.e., directory table) includes the directory tree entries starting from the root except for the entries for files. The namespace replication cost is usually very small due to two observations. First, the number of directories is usually several orders of magnitude smaller than that of files, the replication storage overhead is small. Even for one hundred million directories, the storage footprint on each metadata server is usually less than 10 GB. Meanwhile, the directory operations (e.g., mkdir and rmdir) usually occupies a small portion of total operations in realistic workloads.

**2. Sharded file metadata**: 
In contrast to directories, we distribute all the file metadata across the metadata servers by hashing their file names and assigning them to the inode table shards. We create B-link tree indices for the inode table shards for fast lookup. FalconFS can handle metadata load imbalance or perform computing/storage capacity expansion by migrating shards between metadata servers. The advanced live migration functionality will be open-sourced later. To support high level parallelism both within and among metadata servers, FalconFS creates a large number of shards spreading across the metadata servers where each one may hold a set of shards. Within each metadata server, there is no B-link tree locking contention upon concurrent updates/inserts/deletes/lookups to different co-located shards.

**3. Concurrent Request Merging**: 
FalconFS proposes a novel concurrent request merging framework to scale up per-metadata server throughput. Specifically, FalconFS coalesces the locking and logging overhead among concurrent file/directory operation requests to maximize each metadata server's throughput. FalconFS starts a fixed number of PostgreSQL backends and creates a proxy thread which accepts requests from clients, puts requests into merging queues and dispatches coalesced requests to the backends. Each backend would execute coalesced requests in a batch to amortize logging and locking overhead.
Such proxy architecture also enables FalconFS to support a large number of compute nodes (clients).

**4. Atomicity, Consistency, Isolation and Durability**: 
For single metadata server operations like file open, stat, create and delete, FalconFS leverages PostgreSQL's transaction mechanism to guarantee ACID. For cross-metadata server operations such as directory and rename operations, FalconFS uses two-phase commit protocol to guarantee ACID properties. We propose a light-weight file path locking protocol to resolve conflicts between concurrent file system operations.

## File data store

FalconFS can deploy one or more file data store instances within each compute node to store assigned files on local DRAM and SSDs. Each file data store can also utilize cloud object store such as Huawei cloud OBS as backend to support cost-effective and elastic storage while maintaining high performance for hot/warm data access. The file data store currently does not support data replication and can leverage remote cloud store for high data availability. When configured with cloud object store，newly written data would be persisted into remote cloud store upon file close or fsync calls.

## Client and file system interfaces

As each metadata server can perform file path resolution locally, clients can complete most of file operations in one network round trip time (RTT).  Clients use cached shard mapping to route each metadata request to the appropriate metadata server. In case of membership changes of metadata servers or online migration events of shards, clients would receive errors from the metadata servers and refresh its cached shard mapping.

FalconFS supports standard POSIX API in the user-space through the Linux fuse framework. However, fuse client introduces some noticeable overhead compared to native-kernel client (e.g., Lustre kernel client). We also introduce several optimizations to the fuse module, which significantly alleviates the fuse overhead and the advanced fuse module will be open-sourced in the near future. In order to completely avoid the fuse cost, FalconFS also supports libFS interfaces.


## Copyright
Copyright (c) 2025 Huawei Technologies Co., Ltd.
