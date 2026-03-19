/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "postgres.h"

#include "fmgr.h"
#include "utils/palloc.h"
#include "varatt.h"

#include <unistd.h>
#include "metadb/meta_serialize_interface_helper.h"
#include "perf_counter/falcon_per_request_stat.h"
#include "utils/error_log.h"
#include "utils/falcon_shmem_allocator.h"
#include "utils/snapmgr.h"

static int32_t *g_currentStatIndices = NULL;
static int32_t g_currentStatIndicesCount = 0;

PG_FUNCTION_INFO_V1(falcon_meta_call_by_serialized_shmem_internal);
PG_FUNCTION_INFO_V1(falcon_meta_call_by_serialized_data);

static SerializedData FileMetaProcess(FalconSupportMetaService metaService, int count, char *paramBuffer)
{

    if (count != 1 && !(metaService == MKDIR || metaService == MKDIR_SUB_MKDIR || metaService == MKDIR_SUB_CREATE ||
                        metaService == CREATE || metaService == STAT || metaService == OPEN || metaService == CLOSE ||
                        metaService == UNLINK))
        FALCON_ELOG_ERROR_EXTENDED(ARGUMENT_ERROR, "metaService %d doesn't support batch operation.", metaService);

    SerializedData param;

    if (!SerializedDataInit(&param, paramBuffer, SD_SIZE_T_MAX, SD_SIZE_T_MAX, NULL))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "SerializedDataInit failed.");

    void *data = palloc((sizeof(MetaProcessInfoData) + sizeof(MetaProcessInfoData *)) * count);
    MetaProcessInfoData *infoDataArray = data;
    MetaProcessInfo *infoArray = (MetaProcessInfo *)(infoDataArray + count);
    if (!SerializedDataMetaParamDecode(metaService, count, &param, infoDataArray))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized param is corrupt.");
    for (int i = 0; i < count; i++) {
        infoArray[i] = infoDataArray + i;
        infoDataArray[i].statArrayIndex = -1;
    }
    for (int i = 0; i < count && i < g_currentStatIndicesCount; i++) {
        infoArray[i]->statArrayIndex = g_currentStatIndices[i];
        STAT_CKPT(g_currentStatIndices[i], CKPT_PARAM_DECODE);
    }

    switch (metaService) {
    case MKDIR:
        FalconMkdirHandle(infoArray, count);
        break;
    case MKDIR_SUB_MKDIR:
        FalconMkdirSubMkdirHandle(infoArray, count);
        break;
    case MKDIR_SUB_CREATE:
        FalconMkdirSubCreateHandle(infoArray, count);
        break;
    case CREATE:
        FalconCreateHandle(infoArray, count, false);
        break;
    case STAT:
        FalconStatHandle(infoArray, count);
        break;
    case OPEN:
        FalconOpenHandle(infoArray, count);
        break;
    case CLOSE:
        FalconCloseHandle(infoArray, count);
        break;
    case UNLINK:
        FalconUnlinkHandle(infoArray, count);
        break;
    case READDIR:
        FalconReadDirHandle(infoArray[0]);
        break;
    case OPENDIR:
        FalconOpenDirHandle(infoArray[0]);
        break;
    case RMDIR:
        FalconRmdirHandle(infoArray[0]);
        break;
    case RMDIR_SUB_RMDIR:
        FalconRmdirSubRmdirHandle(infoArray[0]);
        break;
    case RMDIR_SUB_UNLINK:
        FalconRmdirSubUnlinkHandle(infoArray[0]);
        break;
    case RENAME:
        FalconRenameHandle(infoArray[0]);
        break;
    case RENAME_SUB_RENAME_LOCALLY:
        FalconRenameSubRenameLocallyHandle(infoArray[0]);
        break;
    case RENAME_SUB_CREATE:
        FalconRenameSubCreateHandle(infoArray[0]);
        break;
    case UTIMENS:
        FalconUtimeNsHandle(infoArray[0]);
        break;
    case CHOWN:
        FalconChownHandle(infoArray[0]);
        break;
    case CHMOD:
        FalconChmodHandle(infoArray[0]);
        break;
    default:
        FALCON_ELOG_ERROR_EXTENDED(ARGUMENT_ERROR, "unexpected metaService: %d", metaService);
    }

    SerializedData response;
    SerializedDataInit(&response, NULL, 0, 0, &PgMemoryManager);
    if (!SerializedDataMetaResponseEncodeWithPerProcessFlatBufferBuilder(metaService, count, infoDataArray, &response))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "failed when serializing response.");
    for (int i = 0; i < count && i < g_currentStatIndicesCount; i++) {
        int32_t si = g_currentStatIndices[i];
        if (si >= 0 && g_FalconPerRequestStatShmem != NULL)
            StatCheckpoint(si, g_FalconPerRequestStatShmem->statArray[si].checkpointCount);
    }

    return response;
}

static SerializedData KVMetaProcess(FalconSupportMetaService metaService, char *paramBuffer)
{
    SerializedData param;

    if (!SerializedDataInit(&param, paramBuffer, SD_SIZE_T_MAX, SD_SIZE_T_MAX, NULL))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "SerializedDataInit failed.");
    
    KvMetaProcessInfoData infoData = {0};
    infoData.statArrayIndex = -1;
    if (!SerializedKvMetaParamDecode(metaService, &param, &infoData))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized param is corrupt.");
    if (g_currentStatIndicesCount > 0 && g_currentStatIndices != NULL) {
        infoData.statArrayIndex = g_currentStatIndices[0];
        STAT_CKPT(g_currentStatIndices[0], CKPT_PARAM_DECODE);
    }

    switch (metaService) {
        case KV_PUT:
            FalconKvmetaPutHandle(&infoData);
            break;
        case KV_GET:
            FalconKvmetaGetHandle(&infoData);
            break;
        case KV_DEL:
            FalconKvmetaDelHandle(&infoData);
            break;
        default:
            FALCON_ELOG_ERROR_EXTENDED(ARGUMENT_ERROR, "unexpected metaService: %d", metaService);
    }

    SerializedData response;
    SerializedDataInit(&response, NULL, 0, 0, &PgMemoryManager);
    if (!SerializedKvMetaResponseEncodeWithPerProcessFlatBufferBuilder(metaService, &infoData, &response))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "failed when serializing response.");
    if (infoData.statArrayIndex >= 0 && g_FalconPerRequestStatShmem != NULL)
        StatCheckpoint(infoData.statArrayIndex,
                       g_FalconPerRequestStatShmem->statArray[infoData.statArrayIndex].checkpointCount);

    return response;
}

static SerializedData SliceMetaProcess(FalconSupportMetaService metaService, int count, char *paramBuffer)
{
    SerializedData param;

    if (!SerializedDataInit(&param, paramBuffer, SD_SIZE_T_MAX, SD_SIZE_T_MAX, NULL))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "SerializedDataInit failed.");

    void *data = palloc((sizeof(SliceProcessInfoData) + sizeof(SliceProcessInfo)) * count);
    SliceProcessInfoData *infoDataArray = data;
    SliceProcessInfo *infoArray = (SliceProcessInfo *)(infoDataArray + count);
    if (!SerializedSliceParamDecode(metaService, count, &param, infoDataArray))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized param is corrupt.");

    for (int i = 0; i < count; i++) {
        infoArray[i] = infoDataArray + i;
        infoDataArray[i].statArrayIndex = -1;
    }
    for (int i = 0; i < count && i < g_currentStatIndicesCount; i++) {
        infoArray[i]->statArrayIndex = g_currentStatIndices[i];
        STAT_CKPT(g_currentStatIndices[i], CKPT_PARAM_DECODE);
    }

    switch (metaService) {
        case SLICE_PUT:
            FalconSlicePutHandle(infoArray, count);
            break;
        case SLICE_GET:
            FalconSliceGetHandle(infoArray, count);
            break;
        case SLICE_DEL:
            FalconSliceDelHandle(infoArray, count);
            break;
        default:
            FALCON_ELOG_ERROR_EXTENDED(ARGUMENT_ERROR, "unexpected metaService: %d", metaService);
    }

    SerializedData response;
    SerializedDataInit(&response, NULL, 0, 0, &PgMemoryManager);
    if (!SerializedSliceResponseEncodeWithPerProcessFlatBufferBuilder(metaService, count, infoDataArray, &response))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "failed when serializing response.");
    for (int i = 0; i < count && i < g_currentStatIndicesCount; i++) {
        int32_t si = g_currentStatIndices[i];
        if (si >= 0 && g_FalconPerRequestStatShmem != NULL)
            StatCheckpoint(si, g_FalconPerRequestStatShmem->statArray[si].checkpointCount);
    }

    return response;
}

static SerializedData SliceIdProcess(char *paramBuffer)
{
    SerializedData param;

    if (!SerializedDataInit(&param, paramBuffer, SD_SIZE_T_MAX, SD_SIZE_T_MAX, NULL))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "SerializedDataInit failed.");
    
    SliceIdProcessInfoData infoData = {0};
    infoData.statArrayIndex = -1;
    if (!SerializedSliceIdParamDecode(&param, &infoData))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized param is corrupt.");
    if (g_currentStatIndicesCount > 0 && g_currentStatIndices != NULL) {
        infoData.statArrayIndex = g_currentStatIndices[0];
        STAT_CKPT(g_currentStatIndices[0], CKPT_PARAM_DECODE);
    }

    FalconFetchSliceIdHandle(&infoData);

    SerializedData response;
    SerializedDataInit(&response, NULL, 0, 0, &PgMemoryManager);
    if (!SerializedSliceIdResponseEncodeWithPerProcessFlatBufferBuilder(&infoData, &response))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "failed when serializing response.");
    if (infoData.statArrayIndex >= 0 && g_FalconPerRequestStatShmem != NULL)
        StatCheckpoint(infoData.statArrayIndex,
                       g_FalconPerRequestStatShmem->statArray[infoData.statArrayIndex].checkpointCount);

    return response;
}

static SerializedData MetaProcess(FalconSupportMetaService metaService, int count, char *paramBuffer)
{
    if (metaService >= PLAIN_COMMAND && metaService <= CHMOD) {
        return FileMetaProcess(metaService, count, paramBuffer);
    }

    if (metaService >= KV_PUT && metaService <= KV_DEL) {
        return KVMetaProcess(metaService, paramBuffer);
    }

    if (metaService >= SLICE_PUT && metaService <= SLICE_DEL) {
        return SliceMetaProcess(metaService, count, paramBuffer);
    }

    if (metaService == FETCH_SLICE_ID) {
        return SliceIdProcess(paramBuffer);
    }

    FALCON_ELOG_ERROR_EXTENDED(ARGUMENT_ERROR, "metaService %d doesn't support operation.", metaService);
    SerializedData param;
    return param;
}

Datum falcon_meta_call_by_serialized_shmem_internal(PG_FUNCTION_ARGS)
{
    int32_t type = PG_GETARG_INT32(0);
    int32_t count = PG_GETARG_INT32(1);
    uint64_t paramShmemShift = (uint64_t)PG_GETARG_INT64(2);
    int64_t signature = PG_GETARG_INT64(3);
    uint64_t statIndicesShift = (uint64_t)PG_GETARG_INT64(4);

    // set type to FalconMetaServiceType from the send end.
    FalconMetaServiceType metaService = (FalconMetaServiceType)type;
    FalconShmemAllocator *allocator = GetFalconConnectionPoolShmemAllocator();
    if (paramShmemShift > allocator->pageCount * FALCON_SHMEM_ALLOCATOR_PAGE_SIZE)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "paramShmemShift is invalid.");
    char *paramBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, paramShmemShift);

    PushActiveSnapshot(GetTransactionSnapshot());
    if (statIndicesShift != 0 && statIndicesShift < allocator->pageCount * FALCON_SHMEM_ALLOCATOR_PAGE_SIZE) {
        g_currentStatIndices = (int32_t *)FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, statIndicesShift);
        g_currentStatIndicesCount = count;
    } else {
        g_currentStatIndices = NULL;
        g_currentStatIndicesCount = 0;
    }

    for (int32_t si = 0; si < g_currentStatIndicesCount; si++) {
        STAT_CKPT(g_currentStatIndices[si], CKPT_PG_ENTRY);
    }

    int32_t *savedStatIndices = g_currentStatIndices;
    int32_t savedStatIndicesCount = g_currentStatIndicesCount;

    SerializedData response = MetaProcess(metaService, count, paramBuffer);
    PopActiveSnapshot();

    g_currentStatIndices = NULL;
    g_currentStatIndicesCount = 0;

    uint64_t responseShmemShift = FalconShmemAllocatorMalloc(allocator, response.size);
    if (responseShmemShift == 0)
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "FalconShmemAllocMalloc failed. Size: %u.", response.size);
    for (int32_t si = 0; si < savedStatIndicesCount; si++) {
        int32_t idx = savedStatIndices[si];
        if (idx >= 0 && g_FalconPerRequestStatShmem != NULL)
            StatCheckpoint(idx, g_FalconPerRequestStatShmem->statArray[idx].checkpointCount);
    }
    char *responseBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, responseShmemShift);
    FALCON_SHMEM_ALLOCATOR_SET_SIGNATURE(responseBuffer, signature);
    memcpy(responseBuffer, response.buffer, response.size);

    PG_RETURN_INT64(responseShmemShift);
}

Datum falcon_meta_call_by_serialized_data(PG_FUNCTION_ARGS)
{
    int32_t type = PG_GETARG_INT32(0);
    int32_t count = PG_GETARG_INT32(1);
    bytea *param = PG_GETARG_BYTEA_P(2);

    FalconMetaServiceType metaService = (FalconMetaServiceType)type;
    char *paramBuffer = VARDATA_ANY(param);

    PushActiveSnapshot(GetTransactionSnapshot());
    /* This path has no stat indices — ensure MetaProcess sees clean state */
    g_currentStatIndices = NULL;
    g_currentStatIndicesCount = 0;

    SerializedData response = MetaProcess(metaService, count, paramBuffer);
    PopActiveSnapshot();

    bytea *reply = (bytea *)palloc(VARHDRSZ + response.size);
    memcpy(VARDATA_4B(reply), response.buffer, response.size);
    SET_VARSIZE_4B(reply, VARHDRSZ + response.size);

    PG_RETURN_BYTEA_P(reply);
}
