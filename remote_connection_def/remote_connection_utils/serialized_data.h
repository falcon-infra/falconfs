#ifndef FALCON_REMOTE_CONNECTION_DEF_SERIALIZED_DATA_H
#define FALCON_REMOTE_CONNECTION_DEF_SERIALIZED_DATA_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t sd_size_t;

bool SystemIsLittleEndian(void);
sd_size_t ConvertBetweenBigAndLittleEndian(sd_size_t value);

#define SERIALIZED_DATA_DEFAULT_CAPACITY 128

#define SERIALIZED_DATA_ALIGNMENT sizeof(sd_size_t)
#define SERIALIZED_DATA_ALIGNMENT_MASK (SERIALIZED_DATA_ALIGNMENT - 1)

#define SD_SIZE_T_MAX (UINT32_MAX & ~SERIALIZED_DATA_ALIGNMENT_MASK)

typedef struct MemoryManager
{
    void *(*alloc)(size_t);
    void (*free)(void *);
    void *(*realloc)(void *, size_t);
} MemoryManager;

typedef struct SerializedData
{
    char *buffer;
    sd_size_t size;
    sd_size_t capacity;
    MemoryManager *memoryManager;
} SerializedData;

bool SerializedDataInit(SerializedData *data,
                        char *buffer,
                        sd_size_t bufferSize,
                        sd_size_t validBytes,
                        MemoryManager *memoryManager);
void SerializedDataDestroy(SerializedData *data);
void SerializedDataClear(SerializedData *data);

char *SerializedDataApplyForSegment(SerializedData *data, sd_size_t size);
bool SerializedDataAppend(SerializedData *dest, const SerializedData *src);
sd_size_t SerializedDataNextSeveralItemSize(SerializedData *data, sd_size_t start, uint32_t itemCount);

#ifdef FALCON_REMOTE_CONNECTION_DEF_SERIALIZED_DATA_IMPLEMENT

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

MemoryManager DefaultMemoryManager = {.alloc = malloc, .free = free, .realloc = realloc};

bool SystemIsLittleEndian()
{
    static int CheckFlag = 0;
    // 0: unintialized,
    // 1: little endian
    // 2: big endian
    if (CheckFlag == 0) {
        union
        {
            uint32_t i;
            char c[4];
        } test = {0x01020304};
        if (test.c[0] == 4)
            CheckFlag = 1;
        else
            CheckFlag = 2;
    }
    return CheckFlag == 1;
}

sd_size_t ConvertBetweenBigAndLittleEndian(sd_size_t value)
{
    if (sizeof(sd_size_t) == 4) {
        return ((value & 0xFF000000) >> 24) | ((value & 0x00FF0000) >> 8) | ((value & 0x0000FF00) << 8) |
               ((value & 0x000000FF) << 24);
    } else {
        perror("sd_size_t only support uint32_t currently.\n");
        exit(0);
    }
}

static bool SerializedDataEnlarge(SerializedData *data, sd_size_t size);

bool SerializedDataInit(SerializedData *data,
                        char *buffer,
                        sd_size_t bufferSize,
                        sd_size_t validBytes,
                        MemoryManager *memoryManager)
{
    if (buffer) {
        if ((bufferSize & SERIALIZED_DATA_ALIGNMENT_MASK) != 0 || (validBytes & SERIALIZED_DATA_ALIGNMENT_MASK) != 0)
            return false;

        data->memoryManager = memoryManager;
        data->buffer = buffer;
        data->capacity = bufferSize;
        data->size = validBytes;
    } else {
        data->memoryManager = memoryManager ? memoryManager : &DefaultMemoryManager;
        data->buffer = NULL;
        data->capacity = 0;
        data->size = 0;
    }
    return true;
}

void SerializedDataDestroy(SerializedData *data)
{
    if (data->memoryManager && data->buffer) {
        data->memoryManager->free(data->buffer);
    }
}

void SerializedDataClear(SerializedData *data) { data->size = 0; }

char *SerializedDataApplyForSegment(SerializedData *data, sd_size_t size)
{
    // Align to alignment
    size += (~(size & SERIALIZED_DATA_ALIGNMENT_MASK) + 1) & SERIALIZED_DATA_ALIGNMENT_MASK;
    if (!SerializedDataEnlarge(data, data->size + SERIALIZED_DATA_ALIGNMENT + size))
        return NULL;

    /* support multi Segment, skip prev Segment */
    char *p = data->buffer + data->size;
    /* set data length  */
    *(sd_size_t *)p = SystemIsLittleEndian() ? size : ConvertBetweenBigAndLittleEndian(size);
    data->size += SERIALIZED_DATA_ALIGNMENT + size;
    return p + SERIALIZED_DATA_ALIGNMENT;
}

bool SerializedDataAppend(SerializedData *dest, const SerializedData *src)
{
    if (!SerializedDataEnlarge(dest, dest->size + src->size))
        return false;
    memcpy(dest->buffer + dest->size, src->buffer, src->size);
    dest->size += src->size;
    return true;
}

// start must be the start point of next item
sd_size_t SerializedDataNextSeveralItemSize(SerializedData *data, sd_size_t start, uint32_t itemCount)
{
    if ((start & SERIALIZED_DATA_ALIGNMENT_MASK) != 0)
        return -1;
    char *p = data->buffer + start;
    while (itemCount) {
        sd_size_t itemSize = *(sd_size_t *)p;
        if (!SystemIsLittleEndian())
            itemSize = ConvertBetweenBigAndLittleEndian(itemSize);
        if ((itemSize & SERIALIZED_DATA_ALIGNMENT_MASK) != 0)
            return -1;
        p += SERIALIZED_DATA_ALIGNMENT + itemSize;
        if (p > data->buffer + data->size)
            return -1;
        --itemCount;
    }
    return (sd_size_t)(p - data->buffer) - start;
}

static bool SerializedDataEnlarge(SerializedData *data, sd_size_t size)
{
    if (size <= data->capacity)
        return true;
    if (!data->memoryManager)
        return false;
    sd_size_t newCapacity = data->capacity == 0 ? SERIALIZED_DATA_DEFAULT_CAPACITY : data->capacity * 2;
    while (newCapacity < size)
        newCapacity *= 2;
    char *newBuffer = (char *)(data->buffer ? data->memoryManager->realloc(data->buffer, newCapacity)
                                            : data->memoryManager->alloc(newCapacity));
    if (!newBuffer)
        return false;
    data->buffer = newBuffer;
    data->capacity = newCapacity;
    return true;
}

#endif

#ifdef __cplusplus
}
#endif

#endif
