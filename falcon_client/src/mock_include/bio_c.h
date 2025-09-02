#pragma once

#include<cstdint>

typedef enum {
    RET_CACHE_OK = 0,
    RET_CACHE_ERROR = 1,
    OTHERS,
} CResult;

typedef struct
{
    uint64_t location[2];
} ObjLocation;

typedef void (*BioLoadCallback)(void *context, int32_t result);
typedef void (*BioGetCallbackFunc)(void *context, int32_t result, uint32_t realLen);
typedef void (*BioAsyncPutCallback)(void *context, int32_t result);

CResult BioCalcLocation(uint64_t tenantId, uint64_t objectId, ObjLocation *location);

CResult BioAsyncPut(uint64_t tenantId,
                    const char *key,
                    const char *value,
                    uint64_t length,
                    ObjLocation location,
                    BioAsyncPutCallback callback,
                    void *context);

CResult BioAsyncGet(uint64_t tenantId,
                    const char *key,
                    uint64_t offset,
                    uint64_t length,
                    ObjLocation location,
                    char *value,
                    BioGetCallbackFunc callback,
                    void *context);

CResult BioDelete(uint64_t tenantId, const char *key, ObjLocation location);