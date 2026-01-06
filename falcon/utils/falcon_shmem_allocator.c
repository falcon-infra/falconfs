/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "utils/falcon_shmem_allocator.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

int FalconShmemAllocatorInit(FalconShmemAllocator *allocator, char *shmem, uint64_t size)
{
    uint32_t pageCount = (size - sizeof(PaddedAtomic64) * (1 + FALCON_SHMEM_ALLOCATOR_FREE_LIST_COUNT)) /
                         (sizeof(PaddedAtomic64) + FALCON_SHMEM_ALLOCATOR_PAGE_SIZE);
    if (pageCount == 0)
        return -1;

    allocator->shmem = shmem;
    allocator->size = size;
    allocator->pageCount = pageCount;

    allocator->signatureCounter = (PaddedAtomic64 *)shmem;
    allocator->freeListHint = allocator->signatureCounter + 1;
    allocator->pageCntlArray = allocator->freeListHint + FALCON_SHMEM_ALLOCATOR_FREE_LIST_COUNT;
    allocator->allocatableSpaceBase = (char *)(allocator->pageCntlArray + pageCount);
    return 0;
}

int64_t FalconShmemAllocatorGetUniqueSignature(FalconShmemAllocator *allocator)
{
    return (int64_t)atomic_fetch_add_explicit(&allocator->signatureCounter->data, 1, memory_order_relaxed) + 1;
}

static inline uint64_t GetNextPowerOfTwo(uint64_t num)
{
    if (num == 0 || (num & ((uint64_t)1 << 63)) != 0) {
        return 0;
    }
    if (num == 1) {
        return 1;
    }
    return ((uint64_t)1 << (64 - __builtin_clzll(num - 1)));
}

static uint64_t LevelBlockMask[FALCON_SHMEM_ALLOCATOR_FREE_LIST_COUNT] = {0x0000000000000001,
                                                                          0x0000000100000001,
                                                                          0x0001000100010001,
                                                                          0x0101010101010101,
                                                                          0x1111111111111111,
                                                                          0x5555555555555555,
                                                                          0xFFFFFFFFFFFFFFFF};
static uint64_t LevelBlockOccupyBitMap[FALCON_SHMEM_ALLOCATOR_FREE_LIST_COUNT] = {0xFFFFFFFFFFFFFFFF,
                                                                                  0x00000000FFFFFFFF,
                                                                                  0x000000000000FFFF,
                                                                                  0x00000000000000FF,
                                                                                  0x000000000000000F,
                                                                                  0x0000000000000003,
                                                                                  0x0000000000000001};

uint64_t FalconShmemAllocatorMalloc(FalconShmemAllocator *allocator, uint64_t size)
{
    if (size > FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE - sizeof(MemoryHdr)) {
        printf("asked size exceed limit, size: %" PRIu64 ".", size);
        fflush(stdout);
        return 0; // valid shift of allocated buffer cannot be zero, since there must be a memory head before it
    }

    uint64_t requiredSize = size + sizeof(MemoryHdr);
    if (requiredSize < FALCON_SHMEM_ALLOCATOR_MIN_SUPPORT_ALLOC_SIZE)
        requiredSize = FALCON_SHMEM_ALLOCATOR_MIN_SUPPORT_ALLOC_SIZE;
    else
        requiredSize = GetNextPowerOfTwo(requiredSize);

    int level = __builtin_ctzll(FALCON_SHMEM_ALLOCATOR_PAGE_SIZE) - __builtin_ctzll(requiredSize);

    for (int scan = 0; scan < 2; scan++) {
        uint64_t start;
        if (scan == 0) {
            // first scan: scan from freeListHint
            start = atomic_load_explicit(&allocator->freeListHint[level].data, memory_order_relaxed);
        } else {
            // second scan: scan from begin
            start = 0;
        }

        for (uint32_t pageNo = start; pageNo < allocator->pageCount; ++pageNo) {
            uint64_t bitmap = atomic_load_explicit(&allocator->pageCntlArray[pageNo].data, memory_order_relaxed);

            uint64_t allocatedShift = -1;
            bool succeed = false;
            bool pageIsFull = false;
            while (true) {
                uint64_t expected;
                uint64_t desired;
                if (level == 0) {
                    if (bitmap != 0) // some blocks of this page is used
                        break;
                    expected = 0;
                    desired = ~(uint64_t)0;
                    allocatedShift = FALCON_SHMEM_ALLOCATOR_PAGE_SIZE * pageNo;
                } else {
                    expected = bitmap;

                    // Shift several time to get the bitmap of corresponding level
                    int shift = 1;
                    for (int j = FALCON_SHMEM_ALLOCATOR_FREE_LIST_COUNT - 1; j > level; --j) {
                        bitmap = ((bitmap >> shift) | bitmap) & LevelBlockMask[j - 1];
                        shift <<= 1;
                    }

                    if (bitmap == LevelBlockMask[level]) // all of the blocks in this level is used
                        break;

                    int firstEmptyBlockInLevel = __builtin_ctzll(~bitmap & LevelBlockMask[level]);
                    allocatedShift = FALCON_SHMEM_ALLOCATOR_PAGE_SIZE * pageNo +
                                     FALCON_SHMEM_ALLOCATOR_MIN_BLOCK_SIZE * firstEmptyBlockInLevel;
                    if (allocatedShift + requiredSize == FALCON_SHMEM_ALLOCATOR_PAGE_SIZE)
                        pageIsFull = true;
                    desired = expected | (LevelBlockOccupyBitMap[level] << firstEmptyBlockInLevel);
                }

                if (atomic_compare_exchange_strong_explicit(&allocator->pageCntlArray[pageNo].data,
                                                            &expected,
                                                            desired,
                                                            memory_order_relaxed,
                                                            memory_order_relaxed)) {
                    succeed = true;

                    if (scan == 0) {
                        if (pageIsFull || pageNo != start) {
                            // Renew freelistHint if it is not changed, If freellstuint is changed by others,
                            // we don't try to change it, since the process changed freelistHint may have freed
                            // a block, as a result the freelistHint is pointing to a free block as expected.
                            atomic_compare_exchange_strong_explicit(&allocator->freeListHint[level].data,
                                                                    &start,
                                                                    pageIsFull ? pageNo + 1 : pageNo,
                                                                    memory_order_relaxed,
                                                                    memory_order_relaxed);
                        }
                    } else {
                        if (!pageIsFull) {
                            uint64_t freeHint =
                                atomic_load_explicit(&allocator->freeListHint[level].data, memory_order_relaxed);
                            while (true) {
                                if (freeHint <= pageNo) // do nothing as unnecessary
                                    break;
                                if (atomic_compare_exchange_weak_explicit(&allocator->freeListHint[level].data,
                                                                          &freeHint,
                                                                          pageNo,
                                                                          memory_order_relaxed,
                                                                          memory_order_relaxed))
                                    break;
                            }
                        }
                    }

                    break;
                }
                bitmap = expected;
            }

            if (succeed) {
                if (allocatedShift == -1) {
                    printf("unexpected situation in FalconShmemAllocatorMalloc.");
                    fflush(stdout);
                    return 0;
                }
                MemoryHdr *hdr = (MemoryHdr *)FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, allocatedShift);
                hdr->size = size;
                hdr->capacity = requiredSize;
                hdr->signature = 0;
                return allocatedShift + sizeof(MemoryHdr);
            }
        }
    }
    printf("FalconShmemAllocatorMalloc: Cannot find a segment.");
    fflush(stdout);
    return 0;
}

void FalconShmemAllocatorFree(FalconShmemAllocator *allocator, uint64_t shift)
{
    shift -= sizeof(MemoryHdr);
    MemoryHdr *hdr = (MemoryHdr *)FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, shift);
    uint64_t capacity = hdr->capacity;
    if (capacity < FALCON_SHMEM_ALLOCATOR_MIN_SUPPORT_ALLOC_SIZE ||
        capacity > FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE)
        return;
    int level = __builtin_ctzll(FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE) - __builtin_ctzll(capacity);
    if (capacity != (FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE >> level))
        return;

    uint64_t pageNo = shift / FALCON_SHMEM_ALLOCATOR_PAGE_SIZE;
    uint32_t blockNo = shift / FALCON_SHMEM_ALLOCATOR_MIN_BLOCK_SIZE - pageNo * FALCON_SHMEM_ALLOCATOR_STATE_BIT_COUNT;
    uint64_t occupyBitmap = LevelBlockOccupyBitMap[level] << blockNo;
    atomic_fetch_and_explicit(&allocator->pageCntlArray[pageNo].data, ~occupyBitmap, memory_order_relaxed);

    // Renew freeListHint, Maybe this block will be fetch by others immediately before we change
    // freelistHine, but that doesn't matter
    uint64_t freeHint = atomic_load_explicit(&allocator->freeListHint[level].data, memory_order_relaxed);
    while (true) {
        if (freeHint <= pageNo) // do nothing as unnecessary
            break;
        if (atomic_compare_exchange_weak_explicit(&allocator->freeListHint[level].data,
                                                  &freeHint,
                                                  pageNo,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed))
            break;
    }
}

FalconShmemAllocator g_falconConnectionPoolShmemAllocator;
// Get FalconConnectionPoolShmemAllocator, decouple the init and usage of FalconShmemAllocator
FalconShmemAllocator *GetFalconConnectionPoolShmemAllocator(void) { return &g_falconConnectionPoolShmemAllocator; }
