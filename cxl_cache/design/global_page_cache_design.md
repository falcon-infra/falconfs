# FalconFS CXL 2.0 全局页面缓存设计文档

## 一、设计概述

### 1.1 核心思想
基于CXL 2.0内存池化技术，构建跨节点的全局统一页面缓存层，替代传统的Linux Page Cache，实现：
- **全局地址空间**：所有计算节点共享统一的CXL内存池
- **页面级管理**：以4KB/2MB页面为粒度进行缓存管理
- **写穿策略**：写入时同时更新CXL缓存和FalconFS Data Store
- **LRU淘汰**：全局统一的LRU页面淘汰策略
- **本地优先**：优先访问本地CXL内存，其次远程CXL内存
- **CXL一致性**：使用`clflush`/`clflushopt`/`clwb`等指令保证CXL内存一致性

### 1.2 CXL内存特性与挑战

CXL (Compute Express Link) 2.0 提供了以下特性：
- **内存一致性**：CPU缓存与CXL内存之间需要显式同步
- **加载-存储语义**：支持CPU直接load/store访问CXL内存
- **缓存行粒度**：64字节缓存行对齐访问
- **持久性**：CXL内存可配置为持久性内存(PMEM)

**一致性挑战**：
- CPU缓存与CXL内存之间的数据一致性需要显式管理
- 需要正确使用`clflush`/`clflushopt`/`clwb`+`sfence`等指令
- 多节点并发访问CXL内存需要同步机制

### 1.3 架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Application Layer                              │
│                    (AI Training, Data Preprocessing)                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      FalconFS Client (LibFS/FUSE)                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │              CXL Global Page Cache Layer                         │   │
│  │                                                                  │   │
│  │   ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐    │   │
│  │   │ Page Lookup │───▶│  CXL Memory │───▶│  LRU Eviction   │    │   │
│  │   │   (Hash)    │    │    Pool     │    │    Manager      │    │   │
│  │   └─────────────┘    └─────────────┘    └─────────────────┘    │   │
│  │          │                                               │       │   │
│  │          ▼ (miss)                                        ▼       │   │
│  │   ┌─────────────┐                              ┌─────────────┐  │   │
│  │   │ Fetch from  │                              │ Flush to    │  │   │
│  │   │ Data Store  │                              │ Data Store  │  │   │
│  │   └─────────────┘                              └─────────────┘  │   │
│  │                                                                  │   │
│  │   ┌─────────────────────────────────────────────────────────┐   │   │
│  │   │           CXL Consistency Management                     │   │   │
│  │   │  • clflush/clflushopt/clwb for cache line flush         │   │   │
│  │   │  • sfence for store ordering                            │   │   │
│  │   │  • memory barrier for synchronization                   │   │   │
│  │   └─────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    FalconFS Meta Node (PostgreSQL)                       │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │              CXL Page Cache Metadata Tables                      │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌──────────┐  │   │
│  │  │cxl_page     │ │cxl_page_lru │ │cxl_node_pool│ │cxl_inode │  │   │
│  │  │_table       │ │_table       │ │_table       │ │_stats    │  │   │
│  │  │(页面映射)    │ │(LRU链表)     │ │(内存池管理)  │ │(统计信息) │  │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘ └──────────┘  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    FalconFS Data Node (FalconStore)                      │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────┐    │
│  │ Local SSD   │    │  OBS Backend│    │   CXL Memory (Remote)   │    │
│  │   Cache     │    │  (Cold Data)│    │   (via CXL 2.0 Switch)  │    │
│  └─────────────┘    └─────────────┘    └─────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

## 二、CXL一致性管理

### 2.1 缓存行刷新策略

```c
/*
 * CXL Cache Line Flush Strategies
 * 
 * For CXL memory, we need to ensure CPU cache lines are properly
 * flushed to CXL memory for consistency across nodes.
 */

/* Option 1: clflush - Legacy, strong ordering, high latency */
static inline void cxl_clflush(void *addr)
{
    asm volatile("clflush %0" : "+m" (*(volatile char *)addr));
}

/* Option 2: clflushopt - Optimized, weaker ordering, lower latency */
static inline void cxl_clflushopt(void *addr)
{
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)addr));
}

/* Option 3: clwb - Cache Line Write Back, keeps line in cache */
static inline void cxl_clwb(void *addr)
{
    asm volatile(".byte 0x66, 0x0f, 0xae, 0xf0" : : "m" (*(volatile char *)addr));
}

/* Store fence for ordering */
static inline void cxl_sfence(void)
{
    asm volatile("sfence" ::: "memory");
}

/* Memory barrier */
static inline void cxl_mfence(void)
{
    asm volatile("mfence" ::: "memory");
}
```

### 2.2 页面刷新接口

```c
/*
 * Flush a 4KB page to CXL memory
 * Uses clflushopt for better performance, falls back to clflush if needed
 */
static inline void cxl_flush_page(void *page_addr)
{
    char *addr = (char *)page_addr;
    
    /* Flush 64 cache lines (4KB / 64B = 64) */
    for (int i = 0; i < 64; i++) {
        cxl_clflushopt(addr + (i * 64));
    }
    
    /* Ensure all stores are globally visible */
    cxl_sfence();
}

/*
 * Flush a 2MB huge page
 */
static inline void cxl_flush_huge_page(void *page_addr)
{
    char *addr = (char *)page_addr;
    
    /* Flush 32768 cache lines (2MB / 64B = 32768) */
    for (int i = 0; i < 32768; i++) {
        cxl_clflushopt(addr + (i * 64));
    }
    
    cxl_sfence();
}

/*
 * Optimized page copy with CXL flush
 * Copies data and ensures CXL consistency
 */
static inline void cxl_page_copy_and_flush(void *dst, const void *src, size_t len)
{
    /* Use non-temporal stores for large copies to avoid cache pollution */
    if (len >= 4096) {
        /* Use movntdq for non-temporal stores */
        __m128i *d = (__m128i *)dst;
        const __m128i *s = (const __m128i *)src;
        size_t n = len / 16;
        
        while (n--) {
            _mm_stream_si128(d++, _mm_loadu_si128(s++));
        }
        
        /* Ensure non-temporal stores are globally visible */
        cxl_mfence();
    } else {
        /* Small copy: regular memcpy + clflush */
        memcpy(dst, src, len);
        cxl_flush_page(dst);
    }
}
```

## 三、元数据表结构设计

### 3.1 页面映射表 (cxl_page_table)

存储文件页面到CXL物理地址的映射关系。

```sql
CREATE TABLE falcon.falcon_cxl_page_table (
    -- 主键: 文件inode + 页面索引
    inode_id            bigint NOT NULL,
    page_index          bigint NOT NULL,        -- 文件内的页面索引 (offset / page_size)
    
    -- CXL内存位置
    node_id             int NOT NULL,           -- CXL内存所在节点
    pool_id             int NOT NULL DEFAULT 0, -- 内存池ID
    physical_addr       bigint NOT NULL,        -- CXL物理地址 (48-bit CXL address)
    
    -- 页面状态
    page_state          smallint NOT NULL DEFAULT 0,  -- 0=CLEAN, 1=DIRTY, 2=FLUSHING, 3=EVICTING
    
    -- LRU链表指针 (用于快速LRU操作)
    lru_prev_inode_id   bigint,                 -- 前一个页面inode
    lru_prev_page_index bigint,                 -- 前一个页面index
    lru_next_inode_id   bigint,                 -- 后一个页面inode
    lru_next_page_index bigint,                 -- 后一个页面index
    
    -- 元数据
    version             bigint NOT NULL DEFAULT 1,    -- 版本号
    ref_count           int NOT NULL DEFAULT 0,       -- 引用计数
    last_access_node    int,                    -- 最后访问节点
    access_count        bigint NOT NULL DEFAULT 0,    -- 访问次数
    create_time         timestamp DEFAULT now(),
    access_time         timestamp DEFAULT now(),
    
    PRIMARY KEY (inode_id, page_index)
);

-- 索引设计
CREATE INDEX idx_cxl_page_node ON falcon.falcon_cxl_page_table(node_id, pool_id);
CREATE INDEX idx_cxl_page_state ON falcon.falcon_cxl_page_table(page_state);
CREATE INDEX idx_cxl_page_lru ON falcon.falcon_cxl_page_table(access_time);
```

### 3.2 LRU链表管理表 (cxl_page_lru_list)

维护全局LRU链表的头尾指针，支持O(1)的LRU操作。

```sql
CREATE TABLE falcon.falcon_cxl_page_lru_list (
    list_id             int NOT NULL PRIMARY KEY,     -- 0 = Global LRU list
    head_inode_id       bigint,                       -- LRU头部 (最热)
    head_page_index     bigint,
    tail_inode_id       bigint,                       -- LRU尾部 (最冷)
    tail_page_index     bigint,
    total_pages         bigint NOT NULL DEFAULT 0,    -- 总页面数
    dirty_pages         bigint NOT NULL DEFAULT 0,    -- 脏页面数
    clean_pages         bigint NOT NULL DEFAULT 0,    -- 干净页面数
    update_time         timestamp DEFAULT now()
);
```

### 3.3 CXL节点内存池表 (cxl_node_pool)

管理各节点的CXL内存池容量和分配情况。

```sql
CREATE TABLE falcon.falcon_cxl_node_pool (
    node_id             int NOT NULL,
    pool_id             int NOT NULL DEFAULT 0,
    
    -- 内存池配置
    base_physical_addr  bigint NOT NULL,        -- 基地址
    total_size_bytes    bigint NOT NULL,        -- 总大小
    page_size_bytes     int NOT NULL DEFAULT 4096,  -- 页面大小 (4K or 2M)
    total_pages         bigint NOT NULL,        -- 总页面数
    
    -- 使用情况
    used_pages          bigint NOT NULL DEFAULT 0,    -- 已使用页面
    free_pages          bigint NOT NULL,        -- 空闲页面
    dirty_pages         bigint NOT NULL DEFAULT 0,    -- 脏页面
    
    -- 性能指标
    local_access_count  bigint NOT NULL DEFAULT 0,    -- 本地访问次数
    remote_access_count bigint NOT NULL DEFAULT 0,    -- 远程访问次数
    
    -- 状态
    is_active           bool NOT NULL DEFAULT true,
    last_heartbeat      timestamp DEFAULT now(),
    
    PRIMARY KEY (node_id, pool_id)
);
```

### 3.4 空闲页面管理表 (cxl_free_page_list)

管理空闲页面的分配，采用位图或链表方式。

```sql
CREATE TABLE falcon.falcon_cxl_free_page_list (
    node_id             int NOT NULL,
    pool_id             int NOT NULL DEFAULT 0,
    page_offset         bigint NOT NULL,        -- 页面在池内的偏移
    is_allocated        bool NOT NULL DEFAULT false,
    
    PRIMARY KEY (node_id, pool_id, page_offset)
);

-- 快速查找空闲页面的索引
CREATE INDEX idx_free_pages ON falcon.falcon_cxl_free_page_list(node_id, pool_id, is_allocated) 
WHERE is_allocated = false;
```

### 3.5 文件缓存统计表 (cxl_inode_stats)

记录每个文件的缓存命中统计，用于智能预读。

```sql
CREATE TABLE falcon.falcon_cxl_inode_stats (
    inode_id            bigint NOT NULL PRIMARY KEY,
    
    -- 缓存统计
    cached_pages        bigint NOT NULL DEFAULT 0,    -- 当前缓存页面数
    total_cache_hits    bigint NOT NULL DEFAULT 0,    -- 总命中次数
    total_cache_misses  bigint NOT NULL DEFAULT 0,    -- 总未命中次数
    
    -- 访问模式
    sequential_count    bigint NOT NULL DEFAULT 0,    -- 顺序访问次数
    random_count        bigint NOT NULL DEFAULT 0,    -- 随机访问次数
    
    -- 预读策略
    prefetch_window     int NOT NULL DEFAULT 4,       -- 预读窗口大小
    last_access_pattern smallint DEFAULT 0,           -- 0=未知, 1=顺序, 2=随机
    
    -- 时间戳
    create_time         timestamp DEFAULT now(),
    update_time         timestamp DEFAULT now()
);
```

## 四、核心数据结构 (C Header)

### 4.1 页面描述符

```c
/* falcon/include/metadb/cxl_page_cache.h */

#ifndef FALCON_CXL_PAGE_CACHE_H
#define FALCON_CXL_PAGE_CACHE_H

#include "postgres.h"
#include "storage/spin.h"
#include "utils/hsearch.h"
#include "port/atomics.h"

/* Page States */
typedef enum CxlPageState {
    CXL_PAGE_STATE_CLEAN = 0,       /* Page is clean (consistent with storage) */
    CXL_PAGE_STATE_DIRTY = 1,       /* Page is dirty (needs flush) */
    CXL_PAGE_STATE_FLUSHING = 2,    /* Page is being flushed */
    CXL_PAGE_STATE_EVICTING = 3,    /* Page is being evicted */
} CxlPageState;

/* Page Size Support */
typedef enum CxlPageSize {
    CXL_PAGE_SIZE_4K = 4096,
    CXL_PAGE_SIZE_2M = 2097152,
} CxlPageSize;

/* ============================================================================
 * CXL Page Descriptor - In-memory representation of a cached page
 * ============================================================================ */
typedef struct CxlPageDesc {
    /* Page Identity */
    uint64_t inodeId;               /* File inode ID */
    uint64_t pageIndex;             /* Page index within file */
    
    /* CXL Memory Location */
    int32_t nodeId;                 /* Node where page resides */
    int32_t poolId;                 /* Pool ID */
    uint64_t physicalAddr;          /* CXL physical address */
    void *virtualAddr;              /* Mapped virtual address for access */
    
    /* State Management */
    CxlPageState state;             /* Page state */
    pg_atomic_uint64 version;       /* Version for consistency */
    pg_atomic_uint32 refCount;      /* Reference count */
    
    /* LRU List Links (doubly linked list) */
    struct CxlPageDesc *lruPrev;    /* Previous in LRU list */
    struct CxlPageDesc *lruNext;    /* Next in LRU list */
    
    /* Hash Table Linkage */
    struct CxlPageDesc *hashNext;   /* Next in hash bucket */
    
    /* Statistics */
    int32_t lastAccessNode;         /* Last accessing node */
    pg_atomic_uint64 accessCount;   /* Total access count */
    TimestampTz accessTime;         /* Last access timestamp */
    TimestampTz createTime;         /* Creation timestamp */
    
    /* Lock */
    slock_t lock;                   /* Per-page spinlock */
} CxlPageDesc;

/* ============================================================================
 * CXL LRU List - Global LRU management
 * ============================================================================ */
typedef struct CxlLruList {
    CxlPageDesc *head;              /* MRU (Most Recently Used) */
    CxlPageDesc *tail;              /* LRU (Least Recently Used) */
    pg_atomic_uint64 totalPages;    /* Total pages in list */
    pg_atomic_uint64 dirtyPages;    /* Number of dirty pages */
    slock_t lock;                   /* List spinlock */
} CxlLruList;

/* ============================================================================
 * CXL Node Pool - Per-node memory pool management
 * ============================================================================ */
typedef struct CxlNodePool {
    int32_t nodeId;
    int32_t poolId;
    uint64_t baseAddr;              /* Base physical address */
    uint64_t totalSize;             /* Total pool size */
    uint32_t pageSize;              /* Page size (4K or 2M) */
    uint64_t totalPages;
    
    /* Allocation Management */
    pg_atomic_uint64 usedPages;
    pg_atomic_uint64 freePages;
    uint64_t *freePageBitmap;       /* Bitmap for free pages */
    slock_t allocLock;              /* Allocation lock */
    
    /* Virtual Memory Mapping */
    void *mappedBaseAddr;           /* mmap'd base address */
    
    /* Statistics */
    pg_atomic_uint64 localAccesses;
    pg_atomic_uint64 remoteAccesses;
    
    /* CXL Device Handle (for memory operations) */
    void *cxlDeviceHandle;          /* Opaque handle for CXL operations */
} CxlNodePool;

/* ============================================================================
 * CXL Page Cache Manager - Global cache instance
 * ============================================================================ */
typedef struct CxlPageCache {
    /* Hash Table for fast page lookup */
    HTAB *pageHashTable;            /* Key: (inode_id, page_index) */
    slock_t hashTableLock;
    
    /* Global LRU List */
    CxlLruList lruList;
    
    /* Node Pools */
    CxlNodePool **nodePools;        /* Array of node pools */
    int32_t maxNodes;
    int32_t numActivePools;
    slock_t poolsLock;
    
    /* Configuration */
    uint64_t maxCachePages;         /* Maximum cache pages */
    uint64_t dirtyPageThreshold;    /* Threshold for flush trigger */
    float evictionRatio;            /* Ratio of pages to evict when full */
    
    /* Background Workers */
    bool flushWorkerRunning;
    bool evictWorkerRunning;
    
    /* Statistics */
    pg_atomic_uint64 totalHits;
    pg_atomic_uint64 totalMisses;
    pg_atomic_uint64 totalEvictions;
    pg_atomic_uint64 totalFlushes;
} CxlPageCache;

/* ============================================================================
 * Hash Table Key
 * ============================================================================ */
typedef struct CxlPageHashKey {
    uint64_t inodeId;
    uint64_t pageIndex;
} CxlPageHashKey;

/* ============================================================================
 * Page Cache Operations
 * ============================================================================ */

/* Initialization */
extern bool CxlPageCacheInit(uint64_t maxPages);
extern bool CxlPageCacheRegisterNode(int32_t nodeId, int32_t poolId,
                                     uint64_t baseAddr, uint64_t size,
                                     uint32_t pageSize);

/* Page Lookup */
extern CxlPageDesc *CxlPageCacheLookup(uint64_t inodeId, uint64_t pageIndex);
extern CxlPageDesc *CxlPageCacheLookupOrCreate(uint64_t inodeId, 
                                               uint64_t pageIndex,
                                               bool *created);

/* Page Read/Write with CXL consistency */
extern int CxlPageCacheRead(uint64_t inodeId, uint64_t pageIndex,
                            void *buffer, int32_t requestingNode);
extern int CxlPageCacheWrite(uint64_t inodeId, uint64_t pageIndex,
                             void *buffer, int32_t requestingNode);

/* CXL Consistency Operations */
extern void CxlFlushCacheLine(void *addr);
extern void CxlFlushPage(void *pageAddr);
extern void CxlPageCopyAndFlush(void *dst, const void *src, size_t len);
extern void CxlMemoryBarrier(void);

/* LRU Management */
extern void CxlPageCacheMarkAccessed(CxlPageDesc *page);
extern void CxlPageCacheMarkDirty(CxlPageDesc *page);
extern CxlPageDesc *CxlPageCacheEvictPage(void);

/* Flush and Eviction */
extern int CxlPageCacheFlushPage(CxlPageDesc *page);
extern int CxlPageCacheFlushInode(uint64_t inodeId);
extern int CxlPageCacheEvictPages(uint64_t count);

/* Background Operations */
extern void CxlPageCacheBackgroundFlush(void);
extern void CxlPageCacheBackgroundEvict(void);

/* Statistics */
extern void CxlPageCacheGetStats(uint64_t *hits, uint64_t *misses,
                                 uint64_t *evictions, uint64_t *flushes);

/* Constants */
#define FALCON_CXL_MAX_NODES            256
#define FALCON_CXL_MAX_POOLS_PER_NODE   8
#define FALCON_CXL_DEFAULT_PAGE_SIZE    4096
#define FALCON_CXL_HASH_TABLE_SIZE      1048576  /* 1M buckets */
#define FALCON_CXL_DIRTY_THRESHOLD      0.25     /* 25% dirty pages trigger flush */
#define FALCON_CXL_EVICTION_RATIO       0.10     /* Evict 10% when full */

/* Error Codes */
#define CXL_CACHE_SUCCESS               0
#define CXL_CACHE_ERROR_NO_SPACE       -1
#define CXL_CACHE_ERROR_NOT_FOUND      -2
#define CXL_CACHE_ERROR_LOCKED         -3
#define CXL_CACHE_ERROR_IO             -4
#define CXL_CACHE_ERROR_EVICT_FAIL     -5

#endif /* FALCON_CXL_PAGE_CACHE_H */
```

## 五、核心接口实现

### 5.1 CXL一致性原语实现

```c
/* falcon/metadb/cxl_page_cache.c */

#include "metadb/cxl_page_cache.h"
#include "utils/error_log.h"
#include "falcon_store/falcon_store.h"

/* Global cache instance */
static CxlPageCache *g_cxlPageCache = NULL;

/* ============================================================================
 * CXL Cache Line Flush Instructions (x86_64)
 * ============================================================================ */

/* Check if CPU supports clflushopt */
static bool cpu_has_clflushopt = false;
static bool cpu_has_clwb = false;

static void cxl_check_cpu_features(void)
{
    uint32_t eax, ebx, ecx, edx;
    
    /* Check for CLFLUSHOPT (bit 23 of EBX in leaf 7, subleaf 0) */
    __cpuid_count(7, 0, eax, ebx, ecx, edx);
    cpu_has_clflushopt = (ebx >> 23) & 1;
    cpu_has_clwb = (ebx >> 24) & 1;
    
    elog(LOG, "CXL: CPU features - clflushopt=%d, clwb=%d", 
         cpu_has_clflushopt, cpu_has_clwb);
}

/* clflush - Legacy cache line flush */
static inline void cxl_clflush(volatile void *addr)
{
    asm volatile("clflush %0" : "+m" (*(volatile char *)addr));
}

/* clflushopt - Optimized cache line flush */
static inline void cxl_clflushopt(volatile void *addr)
{
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)addr));
}

/* clwb - Cache line write back (keeps line in cache) */
static inline void cxl_clwb(volatile void *addr)
{
    asm volatile(".byte 0x66, 0x0f, 0xae, 0x30" : : "m" (*(volatile char *)addr));
}

/* sfence - Store fence */
static inline void cxl_sfence(void)
{
    asm volatile("sfence" ::: "memory");
}

/* mfence - Memory fence */
static inline void cxl_mfence(void)
{
    asm volatile("mfence" ::: "memory");
}

/*
 * CxlFlushCacheLine - Flush a single cache line (64 bytes)
 * Uses best available instruction
 */
void
CxlFlushCacheLine(void *addr)
{
    if (cpu_has_clwb) {
        cxl_clwb(addr);
    } else if (cpu_has_clflushopt) {
        cxl_clflushopt(addr);
    } else {
        cxl_clflush(addr);
    }
}

/*
 * CxlFlushPage - Flush a 4KB page to CXL memory
 * Flushes all cache lines in the page
 */
void
CxlFlushPage(void *pageAddr)
{
    char *addr = (char *)pageAddr;
    
    /* Flush 64 cache lines (4KB / 64B = 64) */
    for (int i = 0; i < 64; i++) {
        CxlFlushCacheLine(addr + (i * 64));
    }
    
    /* Ensure all stores are globally visible */
    cxl_sfence();
}

/*
 * CxlPageCopyAndFlush - Copy data to CXL memory with consistency
 * 
 * For large copies, use non-temporal stores to avoid cache pollution.
 * For small copies, use regular memcpy + clflush.
 */
void
CxlPageCopyAndFlush(void *dst, const void *src, size_t len)
{
    /* Use non-temporal stores for large copies (>= 4KB) */
    if (len >= 4096) {
        /* Use movntdq for non-temporal stores (16 bytes at a time) */
        __m128i *d = (__m128i *)dst;
        const __m128i *s = (const __m128i *)src;
        size_t n = len / 16;
        
        while (n--) {
            _mm_stream_si128(d++, _mm_loadu_si128(s++));
        }
        
        /* Handle remaining bytes */
        size_t remainder = len % 16;
        if (remainder > 0) {
            memcpy(d, s, remainder);
            CxlFlushCacheLine(d);
        }
        
        /* Ensure non-temporal stores are globally visible */
        cxl_mfence();
    } else {
        /* Small copy: regular memcpy + clflush */
        memcpy(dst, src, len);
        
        /* Flush affected cache lines */
        char *addr = (char *)dst;
        size_t offset = 0;
        while (offset < len) {
            CxlFlushCacheLine(addr + offset);
            offset += 64;  /* Cache line size */
        }
        cxl_sfence();
    }
}

/*
 * CxlMemoryBarrier - Full memory barrier for CXL consistency
 */
void
CxlMemoryBarrier(void)
{
    cxl_mfence();
}
```

### 5.2 页面查找与创建

```c
/*
 * CxlPageCacheLookup - Lookup a page in the cache
 * 
 * Returns: Page descriptor if found, NULL if not in cache
 */
CxlPageDesc *
CxlPageCacheLookup(uint64_t inodeId, uint64_t pageIndex)
{
    CxlPageHashKey key;
    CxlPageDesc *page = NULL;
    bool found = false;
    
    key.inodeId = inodeId;
    key.pageIndex = pageIndex;
    
    /* Lookup in hash table with read lock */
    SpinLockAcquire(&g_cxlPageCache->hashTableLock);
    page = (CxlPageDesc *) hash_search(g_cxlPageCache->pageHashTable,
                                       &key, HASH_FIND, &found);
    SpinLockRelease(&g_cxlPageCache->hashTableLock);
    
    if (found && page != NULL) {
        /* Update access statistics atomically */
        pg_atomic_write_u64(&page->accessCount, 
                           pg_atomic_read_u64(&page->accessCount) + 1);
        page->accessTime = GetCurrentTimestamp();
        
        /* Move to MRU position */
        CxlPageCacheMarkAccessed(page);
        
        return page;
    }
    
    return NULL;
}

/*
 * CxlPageCacheAllocPage - Allocate a new page from CXL memory
 * 
 * Strategy:
 * 1. Try to allocate from local node pool
 * 2. If local is full, try remote nodes with most free space
 * 3. If all pools are full, trigger eviction
 */
static CxlPageDesc *
CxlPageCacheAllocPage(uint64_t inodeId, uint64_t pageIndex, 
                      int32_t preferredNode)
{
    CxlPageDesc *page = NULL;
    CxlNodePool *pool = NULL;
    int32_t targetNode = preferredNode;
    uint64_t pageOffset = 0;
    
    /* Try preferred node first */
    pool = g_cxlPageCache->nodePools[targetNode];
    if (pool != NULL && pg_atomic_read_u64(&pool->freePages) > 0) {
        /* Allocate from local pool */
        SpinLockAcquire(&pool->allocLock);
        
        /* Find first free page in bitmap */
        for (uint64_t i = 0; i < pool->totalPages; i++) {
            if ((pool->freePageBitmap[i / 64] & (1ULL << (i % 64))) == 0) {
                /* Mark as allocated */
                pool->freePageBitmap[i / 64] |= (1ULL << (i % 64));
                pageOffset = i;
                
                pg_atomic_write_u64(&pool->freePages, 
                                   pg_atomic_read_u64(&pool->freePages) - 1);
                pg_atomic_write_u64(&pool->usedPages, 
                                   pg_atomic_read_u64(&pool->usedPages) + 1);
                break;
            }
        }
        
        SpinLockRelease(&pool->allocLock);
    }
    
    /* If local allocation failed, try remote pools */
    if (pageOffset == 0 && pool != NULL) {
        for (int i = 0; i < g_cxlPageCache->maxNodes; i++) {
            if (i == targetNode) continue;
            
            pool = g_cxlPageCache->nodePools[i];
            if (pool == NULL) continue;
            if (pg_atomic_read_u64(&pool->freePages) == 0) continue;
            
            SpinLockAcquire(&pool->allocLock);
            
            for (uint64_t j = 0; j < pool->totalPages; j++) {
                if ((pool->freePageBitmap[j / 64] & (1ULL << (j % 64))) == 0) {
                    pool->freePageBitmap[j / 64] |= (1ULL << (j % 64));
                    pageOffset = j;
                    targetNode = i;
                    
                    pg_atomic_write_u64(&pool->freePages, 
                                       pg_atomic_read_u64(&pool->freePages) - 1);
                    pg_atomic_write_u64(&pool->usedPages, 
                                       pg_atomic_read_u64(&pool->usedPages) + 1);
                    break;
                }
            }
            
            SpinLockRelease(&pool->allocLock);
            
            if (pageOffset != 0) break;
        }
    }
    
    /* If all pools are full, trigger eviction */
    if (pageOffset == 0) {
        uint64_t evictCount = (uint64_t)(FALCON_CXL_EVICTION_RATIO * 
                                         g_cxlPageCache->maxCachePages);
        if (evictCount == 0) evictCount = 1;
        
        if (CxlPageCacheEvictPages(evictCount) < 0) {
            return NULL;
        }
        
        /* Retry allocation */
        return CxlPageCacheAllocPage(inodeId, pageIndex, preferredNode);
    }
    
    /* Initialize page descriptor */
    page = (CxlPageDesc *) MemoryContextAllocZero(TopMemoryContext, 
                                                   sizeof(CxlPageDesc));
    page->inodeId = inodeId;
    page->pageIndex = pageIndex;
    page->nodeId = targetNode;
    page->poolId = pool->poolId;
    page->physicalAddr = pool->baseAddr + (pageOffset * pool->pageSize);
    page->virtualAddr = (char *)pool->mappedBaseAddr + (pageOffset * pool->pageSize);
    page->state = CXL_PAGE_STATE_CLEAN;
    pg_atomic_write_u64(&page->version, 1);
    pg_atomic_write_u32(&page->refCount, 1);
    page->lruPrev = NULL;
    page->lruNext = NULL;
    page->hashNext = NULL;
    page->lastAccessNode = preferredNode;
    pg_atomic_write_u64(&page->accessCount, 1);
    page->accessTime = GetCurrentTimestamp();
    page->createTime = GetCurrentTimestamp();
    SpinLockInit(&page->lock);
    
    return page;
}
```

### 5.3 页面读取流程（带CXL一致性）

```c
/*
 * CxlPageCacheRead - Read a page from CXL cache
 * 
 * Flow:
 * 1. Lookup page in cache
 * 2. If hit, return data from CXL memory (with memory barrier)
 * 3. If miss, allocate page and fetch from Data Store
 * 4. Update LRU and statistics
 */
int
CxlPageCacheRead(uint64_t inodeId, uint64_t pageIndex, 
                 void *buffer, int32_t requestingNode)
{
    CxlPageDesc *page = NULL;
    int ret = CXL_CACHE_SUCCESS;
    
    /* Step 1: Lookup in cache */
    page = CxlPageCacheLookup(inodeId, pageIndex);
    
    if (page != NULL) {
        /* Cache Hit */
        pg_atomic_write_u64(&g_cxlPageCache->totalHits, 
                           pg_atomic_read_u64(&g_cxlPageCache->totalHits) + 1);
        
        /* 
         * Memory barrier to ensure we see the latest data
         * This is crucial for CXL memory consistency
         */
        CxlMemoryBarrier();
        
        /* Read data from CXL memory */
        memcpy(buffer, page->virtualAddr, FALCON_CXL_DEFAULT_PAGE_SIZE);
        
        /* Update node access statistics */
        if (page->nodeId == requestingNode) {
            pg_atomic_write_u64(&g_cxlPageCache->nodePools[page->nodeId]->localAccesses,
                               pg_atomic_read_u64(&g_cxlPageCache->nodePools[page->nodeId]->localAccesses) + 1);
        } else {
            pg_atomic_write_u64(&g_cxlPageCache->nodePools[page->nodeId]->remoteAccesses,
                               pg_atomic_read_u64(&g_cxlPageCache->nodePools[page->nodeId]->remoteAccesses) + 1);
        }
        
        return CXL_CACHE_SUCCESS;
    }
    
    /* Cache Miss */
    pg_atomic_write_u64(&g_cxlPageCache->totalMisses,
                       pg_atomic_read_u64(&g_cxlPageCache->totalMisses) + 1);
    
    /* Step 2: Allocate new page */
    page = CxlPageCacheAllocPage(inodeId, pageIndex, requestingNode);
    if (page == NULL) {
        return CXL_CACHE_ERROR_NO_SPACE;
    }
    
    /* Step 3: Fetch data from FalconFS Data Store */
    ret = FalconStoreReadPage(inodeId, pageIndex, buffer);
    if (ret != 0) {
        /* Free allocated page on error */
        CxlPageCacheFreePage(page);
        return CXL_CACHE_ERROR_IO;
    }
    
    /* Step 4: Write data to CXL memory with flush */
    CxlPageCopyAndFlush(page->virtualAddr, buffer, FALCON_CXL_DEFAULT_PAGE_SIZE);
    
    /* Step 5: Insert into hash table and LRU list */
    CxlPageHashKey key = {inodeId, pageIndex};
    bool found = false;
    
    SpinLockAcquire(&g_cxlPageCache->hashTableLock);
    CxlPageDesc *existing = hash_search(g_cxlPageCache->pageHashTable,
                                        &key, HASH_ENTER, &found);
    if (!found) {
        memcpy(existing, page, sizeof(CxlPageDesc));
    }
    SpinLockRelease(&g_cxlPageCache->hashTableLock);
    
    /* Add to LRU head (MRU) */
    CxlPageCacheAddToLruHead(page);
    
    /* Update statistics */
    CxlPageCacheUpdateInodeStats(inodeId, true, false);
    
    return CXL_CACHE_SUCCESS;
}
```

### 5.4 页面写入流程（写穿+CLFLUSH）

```c
/*
 * CxlPageCacheWrite - Write a page to CXL cache (Write-Through)
 * 
 * Flow:
 * 1. Lookup or create page in cache
 * 2. Write data to CXL memory with flush
 * 3. Mark page as dirty
 * 4. Write data to FalconFS Data Store (write-through)
 * 5. Update LRU and statistics
 */
int
CxlPageCacheWrite(uint64_t inodeId, uint64_t pageIndex,
                  void *buffer, int32_t requestingNode)
{
    CxlPageDesc *page = NULL;
    bool created = false;
    int ret = CXL_CACHE_SUCCESS;
    
    /* Step 1: Lookup or create page */
    page = CxlPageCacheLookup(inodeId, pageIndex);
    if (page == NULL) {
        /* Allocate new page */
        page = CxlPageCacheAllocPage(inodeId, pageIndex, requestingNode);
        if (page == NULL) {
            return CXL_CACHE_ERROR_NO_SPACE;
        }
        created = true;
        
        /* Insert into hash table */
        CxlPageHashKey key = {inodeId, pageIndex};
        bool found = false;
        
        SpinLockAcquire(&g_cxlPageCache->hashTableLock);
        CxlPageDesc *existing = hash_search(g_cxlPageCache->pageHashTable,
                                            &key, HASH_ENTER, &found);
        if (!found) {
            memcpy(existing, page, sizeof(CxlPageDesc));
        }
        SpinLockRelease(&g_cxlPageCache->hashTableLock);
    }
    
    /* Increment reference count */
    pg_atomic_write_u32(&page->refCount,
                       pg_atomic_read_u32(&page->refCount) + 1);
    
    /* Step 2: Write data to CXL memory with flush */
    CxlPageCopyAndFlush(page->virtualAddr, buffer, FALCON_CXL_DEFAULT_PAGE_SIZE);
    
    /* Step 3: Mark page as dirty */
    SpinLockAcquire(&page->lock);
    if (page->state == CXL_PAGE_STATE_CLEAN) {
        page->state = CXL_PAGE_STATE_DIRTY;
        pg_atomic_write_u64(&g_cxlPageCache->lruList.dirtyPages,
                           pg_atomic_read_u64(&g_cxlPageCache->lruList.dirtyPages) + 1);
    }
    pg_atomic_write_u64(&page->version,
                       pg_atomic_read_u64(&page->version) + 1);
    SpinLockRelease(&page->lock);
    
    /* Step 4: Write-through to FalconFS Data Store */
    ret = FalconStoreWritePage(inodeId, pageIndex, buffer);
    if (ret != 0) {
        /* Decrement reference count on error */
        pg_atomic_write_u32(&page->refCount,
                           pg_atomic_read_u32(&page->refCount) - 1);
        return CXL_CACHE_ERROR_IO;
    }
    
    /* Decrement reference count */
    pg_atomic_write_u32(&page->refCount,
                       pg_atomic_read_u32(&page->refCount) - 1);
    
    /* Step 5: Update LRU and statistics */
    CxlPageCacheMarkAccessed(page);
    CxlPageCacheUpdateInodeStats(inodeId, false, true);
    
    /* Check if background flush is needed */
    uint64_t dirtyPages = pg_atomic_read_u64(&g_cxlPageCache->lruList.dirtyPages);
    uint64_t totalPages = pg_atomic_read_u64(&g_cxlPageCache->lruList.totalPages);
    
    if (dirtyPages > g_cxlPageCache->dirtyPageThreshold * totalPages) {
        /* Trigger background flush */
        CxlPageCacheTriggerBackgroundFlush();
    }
    
    return CXL_CACHE_SUCCESS;
}
```

### 5.5 LRU淘汰策略

```c
/*
 * CxlPageCacheEvictPage - Evict the least recently used page
 * 
 * Returns: 0 on success, error code on failure
 */
int
CxlPageCacheEvictPage(void)
{
    CxlPageDesc *victim = NULL;
    int ret = 0;
    
    SpinLockAcquire(&g_cxlPageCache->lruList.lock);
    
    /* Get LRU tail */
    victim = g_cxlPageCache->lruList.tail;
    if (victim == NULL) {
        SpinLockRelease(&g_cxlPageCache->lruList.lock);
        return CXL_CACHE_ERROR_NOT_FOUND;
    }
    
    /* Check if page can be evicted (not locked, not being flushed) */
    SpinLockAcquire(&victim->lock);
    
    if (pg_atomic_read_u32(&victim->refCount) > 0 || 
        victim->state == CXL_PAGE_STATE_FLUSHING) {
        /* Page is in use, find next candidate */
        SpinLockRelease(&victim->lock);
        
        victim = victim->lruPrev;
        while (victim != NULL) {
            SpinLockAcquire(&victim->lock);
            if (pg_atomic_read_u32(&victim->refCount) == 0 && 
                victim->state != CXL_PAGE_STATE_FLUSHING) {
                break;
            }
            SpinLockRelease(&victim->lock);
            victim = victim->lruPrev;
        }
    }
    
    if (victim == NULL) {
        SpinLockRelease(&g_cxlPageCache->lruList.lock);
        return CXL_CACHE_ERROR_LOCKED;
    }
    
    /* Mark as evicting */
    victim->state = CXL_PAGE_STATE_EVICTING;
    
    /* Remove from LRU list */
    if (victim->lruPrev != NULL) {
        victim->lruPrev->lruNext = victim->lruNext;
    } else {
        g_cxlPageCache->lruList.tail = victim->lruNext;
    }
    
    if (victim->lruNext != NULL) {
        victim->lruNext->lruPrev = victim->lruPrev;
    } else {
        g_cxlPageCache->lruList.head = victim->lruPrev;
    }
    
    /* Update counts */
    pg_atomic_write_u64(&g_cxlPageCache->lruList.totalPages,
                       pg_atomic_read_u64(&g_cxlPageCache->lruList.totalPages) - 1);
    
    if (victim->state == CXL_PAGE_STATE_DIRTY) {
        pg_atomic_write_u64(&g_cxlPageCache->lruList.dirtyPages,
                           pg_atomic_read_u64(&g_cxlPageCache->lruList.dirtyPages) - 1);
    }
    
    SpinLockRelease(&victim->lock);
    SpinLockRelease(&g_cxlPageCache->lruList.lock);
    
    /* Flush if dirty */
    if (victim->state == CXL_PAGE_STATE_DIRTY) {
        ret = CxlPageCacheFlushPage(victim);
        if (ret != 0) {
            /* Flush failed, put back to LRU? For now, just free */
            elog(WARNING, "CXL: Failed to flush page before eviction");
        }
    }
    
    /* Remove from hash table */
    CxlPageHashKey key = {victim->inodeId, victim->pageIndex};
    
    SpinLockAcquire(&g_cxlPageCache->hashTableLock);
    hash_search(g_cxlPageCache->pageHashTable, &key, HASH_REMOVE, NULL);
    SpinLockRelease(&g_cxlPageCache->hashTableLock);
    
    /* Free CXL memory */
    CxlPageCacheFreePage(victim);
    
    pg_atomic_write_u64(&g_cxlPageCache->totalEvictions,
                       pg_atomic_read_u64(&g_cxlPageCache->totalEvictions) + 1);
    
    return CXL_CACHE_SUCCESS;
}

/*
 * CxlPageCacheMarkAccessed - Move page to MRU position
 */
void
CxlPageCacheMarkAccessed(CxlPageDesc *page)
{
    SpinLockAcquire(&g_cxlPageCache->lruList.lock);
    
    /* Remove from current position */
    if (page->lruPrev != NULL) {
        page->lruPrev->lruNext = page->lruNext;
    } else {
        /* Already at head (MRU) */
        SpinLockRelease(&g_cxlPageCache->lruList.lock);
        return;
    }
    
    if (page->lruNext != NULL) {
        page->lruNext->lruPrev = page->lruPrev;
    } else {
        g_cxlPageCache->lruList.tail = page->lruPrev;
    }
    
    /* Add to head (MRU) */
    page->lruNext = g_cxlPageCache->lruList.head;
    page->lruPrev = NULL;
    
    if (g_cxlPageCache->lruList.head != NULL) {
        g_cxlPageCache->lruList.head->lruPrev = page;
    }
    
    g_cxlPageCache->lruList.head = page;
    
    SpinLockRelease(&g_cxlPageCache->lruList.lock);
}

/*
 * CxlPageCacheAddToLruHead - Add page to MRU position
 */
static void
CxlPageCacheAddToLruHead(CxlPageDesc *page)
{
    SpinLockAcquire(&g_cxlPageCache->lruList.lock);
    
    page->lruNext = g_cxlPageCache->lruList.head;
    page->lruPrev = NULL;
    
    if (g_cxlPageCache->lruList.head != NULL) {
        g_cxlPageCache->lruList.head->lruPrev = page;
    }
    
    g_cxlPageCache->lruList.head = page;
    
    if (g_cxlPageCache->lruList.tail == NULL) {
        g_cxlPageCache->lruList.tail = page;
    }
    
    pg_atomic_write_u64(&g_cxlPageCache->lruList.totalPages,
                       pg_atomic_read_u64(&g_cxlPageCache->lruList.totalPages) + 1);
    
    SpinLockRelease(&g_cxlPageCache->lruList.lock);
}
```

## 六、与FalconFS的集成

### 6.1 修改FalconStore读写接口

```c
/* falcon_store/src/falcon_store/falcon_store.cpp */

/*
 * Modified FalconStore read with CXL cache integration
 */
int FalconStore::ReadPage(uint64_t inodeId, uint64_t pageIndex, 
                          void *buffer, size_t size)
{
    int ret = 0;
    
#ifdef ENABLE_CXL_CACHE
    /* Try CXL cache first */
    ret = CxlPageCacheRead(inodeId, pageIndex, buffer, GetCurrentNodeId());
    if (ret == CXL_CACHE_SUCCESS) {
        return FALCON_SUCCESS;
    }
    /* Cache miss, fall through to disk read */
#endif
    
    /* Original disk read logic */
    ret = DiskCacheRead(inodeId, pageIndex, buffer, size);
    
    return ret;
}

/*
 * Modified FalconStore write with CXL cache integration
 */
int FalconStore::WritePage(uint64_t inodeId, uint64_t pageIndex,
                           void *buffer, size_t size)
{
    int ret = 0;
    
#ifdef ENABLE_CXL_CACHE
    /* Write to CXL cache (write-through with clflush) */
    ret = CxlPageCacheWrite(inodeId, pageIndex, buffer, GetCurrentNodeId());
    if (ret != CXL_CACHE_SUCCESS) {
        return FALCON_ERROR;
    }
#endif
    
    /* Also write to disk cache (for persistence) */
    ret = DiskCacheWrite(inodeId, pageIndex, buffer, size);
    
    return ret;
}
```

### 6.2 SQL函数接口

```sql
-- Register CXL node pool
CREATE FUNCTION pg_catalog.falcon_cxl_register_pool(
    node_id int,
    pool_id int,
    base_addr bigint,
    pool_size bigint,
    page_size int DEFAULT 4096
) RETURNS bigint
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$falcon_cxl_register_pool$$;

-- Unregister CXL node pool
CREATE FUNCTION pg_catalog.falcon_cxl_unregister_pool(
    node_id int,
    pool_id int
) RETURNS bigint
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$falcon_cxl_unregister_pool$$;

-- Get cache statistics
CREATE FUNCTION pg_catalog.falcon_cxl_cache_stats()
RETURNS TABLE(
    total_pages bigint,
    dirty_pages bigint,
    cache_hits bigint,
    cache_misses bigint,
    hit_ratio float
)
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$falcon_cxl_cache_stats$$;

-- Flush all dirty pages for inode
CREATE FUNCTION pg_catalog.falcon_cxl_flush_inode(inode_id bigint)
RETURNS bigint
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$falcon_cxl_flush_inode$$;

-- Invalidate all pages for inode
CREATE FUNCTION pg_catalog.falcon_cxl_invalidate_inode(inode_id bigint)
RETURNS bigint
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$falcon_cxl_invalidate_inode$$;
```

## 七、性能优化策略

### 7.1 预读策略

```c
/*
 * Sequential Read Detection and Prefetch
 */
void CxlPageCachePrefetch(uint64_t inodeId, uint64_t currentPageIndex,
                          int32_t requestingNode)
{
    CxlInodeStats *stats = CxlGetInodeStats(inodeId);
    
    /* Detect sequential access pattern */
    if (stats->lastAccessedPage == currentPageIndex - 1) {
        stats->sequentialCount++;
        stats->accessPattern = ACCESS_PATTERN_SEQUENTIAL;
        
        /* Increase prefetch window */
        if (stats->prefetchWindow < MAX_PREFETCH_WINDOW) {
            stats->prefetchWindow *= 2;
        }
    } else {
        stats->randomCount++;
        stats->accessPattern = ACCESS_PATTERN_RANDOM;
        stats->prefetchWindow = DEFAULT_PREFETCH_WINDOW;
    }
    
    stats->lastAccessedPage = currentPageIndex;
    
    /* Issue prefetch for next pages */
    if (stats->accessPattern == ACCESS_PATTERN_SEQUENTIAL) {
        for (int i = 1; i <= stats->prefetchWindow; i++) {
            uint64_t prefetchPage = currentPageIndex + i;
            
            /* Skip if already in cache */
            if (CxlPageCacheLookup(inodeId, prefetchPage) != NULL) {
                continue;
            }
            
            /* Async prefetch from Data Store to CXL */
            CxlPageCachePrefetchAsync(inodeId, prefetchPage, requestingNode);
        }
    }
}
```

### 7.2 NUMA-Aware分配

```c
/*
 * NUMA-aware page allocation
 * Prefer local NUMA node, fallback to remote with lowest latency
 */
static CxlNodePool *
CxlSelectOptimalPool(int32_t requestingNode, uint64_t inodeId, 
                     uint64_t pageIndex)
{
    CxlNodePool *localPool = g_cxlPageCache->nodePools[requestingNode];
    
    /* Try local NUMA node first */
    if (localPool != NULL && pg_atomic_read_u64(&localPool->freePages) > 0) {
        return localPool;
    }
    
    /* Find remote pool with most free pages and lowest latency */
    CxlNodePool *bestPool = NULL;
    uint64_t maxScore = 0;
    
    for (int i = 0; i < g_cxlPageCache->maxNodes; i++) {
        if (i == requestingNode) continue;
        
        CxlNodePool *pool = g_cxlPageCache->nodePools[i];
        if (pool == NULL) continue;
        
        uint64_t freePages = pg_atomic_read_u64(&pool->freePages);
        if (freePages == 0) continue;
        
        /* Score = free_pages / (latency + 1) */
        uint64_t latency = CxlGetLatency(requestingNode, i);
        uint64_t score = freePages * 100 / (latency + 1);
        
        if (score > maxScore) {
            maxScore = score;
            bestPool = pool;
        }
    }
    
    return bestPool;
}
```

## 八、部署配置

### 8.1 配置文件 (config.json)

```json
{
    "cxl_cache": {
        "enabled": true,
        "page_size": 4096,
        "max_cache_pages": 268435456,
        "dirty_page_threshold": 0.25,
        "eviction_ratio": 0.10,
        "prefetch_enabled": true,
        "prefetch_window": 4,
        "numa_aware": true,
        "background_flush_interval_ms": 100,
        "background_evict_interval_ms": 1000,
        "use_clflushopt": true,
        "use_clwb": true,
        "use_nt_stores": true
    },
    "cxl_pools": [
        {
            "node_id": 0,
            "pool_id": 0,
            "base_addr": "0x100000000000",
            "size": "512GB",
            "numa_node": 0,
            "mmap_path": "/dev/dax0.0"
        },
        {
            "node_id": 1,
            "pool_id": 0,
            "base_addr": "0x120000000000",
            "size": "512GB",
            "numa_node": 1,
            "mmap_path": "/dev/dax1.0"
        }
    ]
}
```

## 九、总结

本设计实现了基于CXL 2.0的全局页面缓存系统，核心特点：

1. **统一地址空间**：所有节点共享CXL内存池，实现真正的全局缓存
2. **页面级管理**：以4KB/2MB页面为粒度，支持细粒度的缓存控制
3. **写穿策略**：保证数据一致性，CXL缓存和Data Store同步更新
4. **CXL一致性**：使用`clflush`/`clflushopt`/`clwb`+`sfence`保证CXL内存一致性
5. **智能LRU**：全局LRU淘汰，支持NUMA感知和负载均衡
6. **高性能访问**：本地CXL内存访问延迟 < 200ns，远程访问 < 1μs
7. **无缝集成**：与现有FalconFS架构完美融合，支持渐进式部署

关键优化点：
- 使用`clflushopt`替代`clflush`降低刷新延迟
- 使用`clwb`保持缓存行在CPU缓存中
- 使用非临时存储(movnt)避免大拷贝时的缓存污染
- 内存屏障确保多节点一致性
