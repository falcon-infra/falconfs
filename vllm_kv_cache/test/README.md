# FalconFS KV Cache 测试指南

## 概述

本测试套件用于验证 FalconFS KV Cache 内存池的完整功能，基于 v4_final 设计文档。

**核心设计原则**：
- ✅ 所有接口都使用批量操作（batch_* 前缀）
- ✅ Bitmap 在 DN 管理（不是 Store）
- ✅ Client 直接连接 DN（不经过 CN）
- ✅ 2 RTT 完成操作（DN 元数据 + Store 数据）

## 测试架构

```
┌──────────────────────────────────────────────────────────────┐
│                    vLLM Inference Node                        │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Python 测试程序                                      │   │
│  │  - test_offloading_manager.py                        │   │
│  └──────────────────────────────────────────────────────┘   │
│                          │                                   │
│                          ▼                                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  FalconFSOffloadingManager (Python)                   │   │
│  │  - batch_lookup(keys)                                │   │
│  │  - batch_prepare_store(keys)                         │   │
│  │  - batch_complete_store(keys, data)                  │   │
│  │  - batch_prepare_load(keys)                          │   │
│  │  - batch_complete_load(keys)                         │   │
│  │  - batch_touch(keys)                                 │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
          │                    │
          │ RTT 1 (元数据)     │ RTT 2 (数据)
          ▼                    ▼
┌──────────────────┐  ┌──────────────────────────────────────┐
│  DN (直接连接)    │  │  FalconFS Store (同机或远程)          │
│                  │  │                                      │
│  PostgreSQL 进程 │  │  ┌────────────┐                      │
│  内管理：         │  │  │ Store 0    │                      │
│  ✅ 元数据表      │  │  │            │                      │
│  ✅ Bitmap       │  │  │ batch_     │                      │
│     (本地)       │  │  │ write_     │                      │
│  ✅ Lease        │  │  │ block()    │                      │
│  ✅ LRU          │  │  │            │                      │
│                  │  │  │ batch_     │                      │
│                  │  │  │ read_      │                      │
│                  │  │  │ block()    │                      │
│                  │  │  └────────────┘                      │
└──────────────────┘  └──────────────────────────────────────┘
```

## 测试文件对应关系

| Phase | 测试文件 | 测试内容 | 状态 |
|-------|---------|---------|------|
| 1 | `test_store_kv_memory_pool.cpp` | Store 内存池批量读写 | 📝 待实现 |
| 2 | `test_metadata_dn.cpp` | DN 元数据（Bitmap/Lease/LRU） | 📝 待实现 |
| 3 | `test_dn_brpc_service.cpp` | DN BRPC 批量服务 | 📝 待实现 |
| 4 | `test_pybind11.py` | pybind11 绑定 | 📝 待实现 |
| 5+ | `test_offloading_manager.py` | OffloadingManager 完整批量接口 | ✅ Mock 完成 |

## 测试环境要求

### 硬件要求

| 组件 | 最低配置 | 推荐配置 |
|------|---------|---------|
| CPU | 8 核 | 16 核 |
| RAM | 16GB | 32GB |
| SSD | 100GB | 200GB |

### 软件要求

- Ubuntu 22.04 / 24.04 或 openEuler 22.03 / 24.03
- PostgreSQL 14+
- Python 3.10+
- GCC 14+ (编译 FalconFS)
- Google Test (C++ 单元测试)

## 测试执行

### Phase 1: Store 内存池测试

```bash
# 编译
cd build
make test_store_kv_memory_pool

# 运行
./tests/kv_cache/test_store_kv_memory_pool
```

**测试内容**：
- ✅ 批量写入和读取（batch_write_block / batch_read_block）
- ✅ 大批量写入性能（1000 blocks）
- ✅ 大批量读取性能（1000 blocks）
- ✅ 淘汰到 SSD（evict_to_ssd）
- ✅ 从 SSD 批量回读（batch_read_from_ssd）
- ✅ 并发批量写入

### Phase 2: DN 元数据测试

```bash
# 启动 FalconFS
bash scripts/falcon_distributed_test.sh start

# 编译
make test_metadata_dn

# 运行
./tests/kv_cache/test_metadata_dn
```

**测试内容**：
- ✅ 批量分配（batch_allocate_with_lease）
- ✅ 批量查询（batch_lookup_with_lease）
- ✅ 批量更新状态（batch_update_block_status）
- ✅ 批量续约（batch_renew_lease）
- ✅ Lease 过期机制
- ✅ LRU 淘汰
- ✅ 亲和性分配（优先同机 Store）

### Phase 3: DN BRPC 服务测试

```bash
# 编译
make test_dn_brpc_service

# 运行
./tests/kv_cache/test_dn_brpc_service
```

**测试内容**：
- ✅ batch_lookup_with_lease RPC
- ✅ batch_allocate_with_lease RPC
- ✅ batch_update_block_status RPC
- ✅ batch_renew_lease RPC
- ✅ 并发批量请求
- ✅ 错误处理（部分失败）

### Phase 4: Python 客户端测试

```bash
# 安装 Python 包
cd vllm_kv_cache
pip install -e .

# 运行测试
python test/test_pybind11.py
```

**测试内容**：
- ✅ DNClient.batch_lookup_with_lease
- ✅ DNClient.batch_allocate_with_lease
- ✅ DNClient.batch_update_block_status
- ✅ DNClient.batch_renew_lease
- ✅ StoreClient.batch_write_block
- ✅ StoreClient.batch_read_block
- ✅ StoreClient.batch_read_from_ssd

### Phase 5: OffloadingManager 完整测试

```bash
# 运行测试（Mock 模式）
python vllm_kv_cache/test/test_offloading_manager.py

# 运行测试（真实模式，需要完整实现）
python vllm_kv_cache/test/test_offloading_manager.py
```

**测试内容**：
- ✅ Test 1: Batch Lookup Miss
- ✅ Test 2: Batch Prepare Store
- ✅ Test 3: Batch Complete Store
- ✅ Test 4: Batch Lookup Hit
- ✅ Test 5: Batch Prepare Load
- ✅ Test 6: Batch Complete Load
- ✅ Test 7: Batch Touch
- ✅ Test 8: Complete Store → Load Cycle
- ✅ Test 9: Error Handling
- ✅ Test 10: Performance Comparison

## 性能目标

| 接口 | 批量大小 | 目标延迟 | RTT 数 |
|------|---------|---------|--------|
| batch_lookup | 10 keys | < 100μs | 2 |
| batch_prepare_store | 10 keys | < 60μs | 1 |
| batch_complete_store | 10 keys | < 120μs | 2 |
| batch_prepare_load | 10 keys | < 200μs | 2 |
| batch_complete_load | 10 keys | < 60μs | 1 |
| batch_touch | 10 keys | < 60μs | 1 |

## 测试开发流程

### 1. 实现 Phase 1（Store 内存池）

```bash
# 1. 实现 KVMemoryPool
cd falcon_store/src
# 创建 kv_memory_pool.h/.cpp

# 2. 实现批量接口
# - batch_write_block(offsets, data_list)
# - batch_read_block(offsets)
# - evict_to_ssd(offset, path)
# - batch_read_from_ssd(paths)

# 3. 编译测试
cd build
make test_store_kv_memory_pool

# 4. 运行测试
./tests/kv_cache/test_store_kv_memory_pool
```

### 2. 实现 Phase 2（DN 元数据）

```bash
# 1. 实现 PostgreSQL C API
cd falcon/metadb
# 创建 kv_block_meta.c

# 2. 实现 Bitmap/Lease/LRU
cd falcon/include/metadb
# 创建 bitmap_allocator.h/.cpp
# 创建 lease_manager.h/.cpp
# 创建 lru_manager.h/.cpp

# 3. 实现 MetadataDN 服务
# 创建 metadata_dn.h/.cpp

# 4. 编译测试
cd build
make test_metadata_dn

# 5. 运行测试
./tests/kv_cache/test_metadata_dn
```

### 3. 实现 Phase 3-5

按照相同流程，依次实现 BRPC 服务、Python 客户端、OffloadingManager。

## 调试技巧

### 1. 启用详细日志

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### 2. 查看 BRPC 日志

```bash
# DN 日志
tail -f /home/junbin/junbinkang/falconfs/log/cnlogfile0.log

# Store 日志
tail -f /home/junbin/junbinkang/falconfs/log/store.log
```

### 3. 性能分析

```bash
# 使用 perf 分析
perf record -g ./tests/kv_cache/test_store_kv_memory_pool
perf report
```

## 常见问题

### Q1: Mock 测试和真实测试的区别？

**Mock 测试**：
- 使用 Python 模拟实现
- 不依赖网络和 C++ 代码
- 用于验证接口调用流程

**真实测试**：
- 使用 pybind11 绑定 C++ 代码
- 依赖完整的 BRPC 服务
- 用于验证性能和正确性

### Q2: 如何切换 Mock 和真实模式？

```python
# 自动检测
try:
    from falconfs_kv import FalconFSOffloadingManager
    USE_MOCK = False
except ImportError:
    USE_MOCK = True
```

### Q3: 批量接口为什么比单个接口快？

**单个接口**：
```
Client → DN (RTT 1)
Client → DN (RTT 2)
Client → DN (RTT 3)
...
```

**批量接口**：
```
Client → DN (RTT 1, 处理 10 keys)
```

**性能提升**：10x - 50x（取决于批量大小）

## 下一步

1. ✅ 完成设计文档（v4_final）
2. ✅ 完成开发测试计划（DEVELOPMENT_AND_TEST_PLAN.md）
3. ✅ 完成测试文件框架
4. 📝 开始 Phase 1 实现（Store 内存池）
