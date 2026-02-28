/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "postgres.h"

#include "fmgr.h"
#include "utils/palloc.h"
#include "varatt.h"

#include <unistd.h>
#include "metadb/meta_serialize_interface_helper.h"
#include "perf_counter/perf_macros.h"
#include "utils/error_log.h"
#include "utils/falcon_shmem_allocator.h"
#include "utils/snapmgr.h"

PG_FUNCTION_INFO_V1(falcon_meta_call_by_serialized_shmem_internal);
PG_FUNCTION_INFO_V1(falcon_meta_call_by_serialized_data);

static SerializedData FileMetaProcess(FalconSupportMetaService metaService, int count, char *paramBuffer)
{
    FalconPerfLatencyShmem *perf = g_FalconPerfLatencyShmem;

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
    PERF_LATENCY_BEGIN(param_decode, perf ? &perf->paramDecodeLatency : NULL);
    if (!SerializedDataMetaParamDecode(metaService, count, &param, infoDataArray))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized param is corrupt.");
    PERF_LATENCY_END(param_decode);
    for (int i = 0; i < count; i++)
        infoArray[i] = infoDataArray + i;

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
    PERF_LATENCY_BEGIN(response_encode, perf ? &perf->responseEncodeLatency : NULL);
    if (!SerializedDataMetaResponseEncodeWithPerProcessFlatBufferBuilder(metaService, count, infoDataArray, &response))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "failed when serializing response.");
    PERF_LATENCY_END(response_encode);

    return response;
}

static SerializedData KVMetaProcess(FalconSupportMetaService metaService, char *paramBuffer)
{
    FalconPerfLatencyShmem *perf = g_FalconPerfLatencyShmem;
    SerializedData param;

    if (!SerializedDataInit(&param, paramBuffer, SD_SIZE_T_MAX, SD_SIZE_T_MAX, NULL))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "SerializedDataInit failed.");
    
    KvMetaProcessInfoData infoData = {0};
    PERF_LATENCY_BEGIN(param_decode, perf ? &perf->paramDecodeLatency : NULL);
    if (!SerializedKvMetaParamDecode(metaService, &param, &infoData))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized param is corrupt.");
    PERF_LATENCY_END(param_decode);

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
    PERF_LATENCY_BEGIN(response_encode, perf ? &perf->responseEncodeLatency : NULL);
    if (!SerializedKvMetaResponseEncodeWithPerProcessFlatBufferBuilder(metaService, &infoData, &response))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "failed when serializing response.");
    PERF_LATENCY_END(response_encode);

    return response;
}

static SerializedData SliceMetaProcess(FalconSupportMetaService metaService, int count, char *paramBuffer)
{
    FalconPerfLatencyShmem *perf = g_FalconPerfLatencyShmem;
    SerializedData param;

    if (!SerializedDataInit(&param, paramBuffer, SD_SIZE_T_MAX, SD_SIZE_T_MAX, NULL))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "SerializedDataInit failed.");

    void *data = palloc((sizeof(SliceProcessInfoData) + sizeof(SliceProcessInfo)) * count);
    SliceProcessInfoData *infoDataArray = data;
    SliceProcessInfo *infoArray = (SliceProcessInfo *)(infoDataArray + count);
    PERF_LATENCY_BEGIN(param_decode, perf ? &perf->paramDecodeLatency : NULL);
    if (!SerializedSliceParamDecode(metaService, count, &param, infoDataArray))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized param is corrupt.");
    PERF_LATENCY_END(param_decode);

    for (int i = 0; i < count; i++) {
        infoArray[i] = infoDataArray + i;
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
    PERF_LATENCY_BEGIN(response_encode, perf ? &perf->responseEncodeLatency : NULL);
    if (!SerializedSliceResponseEncodeWithPerProcessFlatBufferBuilder(metaService, count, infoDataArray, &response))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "failed when serializing response.");
    PERF_LATENCY_END(response_encode);

    return response;
}

static SerializedData SliceIdProcess(char *paramBuffer)
{
    FalconPerfLatencyShmem *perf = g_FalconPerfLatencyShmem;
    SerializedData param;

    if (!SerializedDataInit(&param, paramBuffer, SD_SIZE_T_MAX, SD_SIZE_T_MAX, NULL))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "SerializedDataInit failed.");
    
    SliceIdProcessInfoData infoData = {0};
    PERF_LATENCY_BEGIN(param_decode, perf ? &perf->paramDecodeLatency : NULL);
    if (!SerializedSliceIdParamDecode(&param, &infoData))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized param is corrupt.");
    PERF_LATENCY_END(param_decode);

    FalconFetchSliceIdHandle(&infoData);

    SerializedData response;
    SerializedDataInit(&response, NULL, 0, 0, &PgMemoryManager);
    PERF_LATENCY_BEGIN(response_encode, perf ? &perf->responseEncodeLatency : NULL);
    if (!SerializedSliceIdResponseEncodeWithPerProcessFlatBufferBuilder(&infoData, &response))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "failed when serializing response.");
    PERF_LATENCY_END(response_encode);

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
    FalconPerfLatencyShmem *perf = g_FalconPerfLatencyShmem;
    int32_t type = PG_GETARG_INT32(0);
    int32_t count = PG_GETARG_INT32(1);
    uint64_t paramShmemShift = (uint64_t)PG_GETARG_INT64(2);
    int64_t signature = PG_GETARG_INT64(3);

    // set type to FalconMetaServiceType from the send end.
    FalconMetaServiceType metaService = (FalconMetaServiceType)type;
    FalconShmemAllocator *allocator = GetFalconConnectionPoolShmemAllocator();
    if (paramShmemShift > allocator->pageCount * FALCON_SHMEM_ALLOCATOR_PAGE_SIZE)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "paramShmemShift is invalid.");
    char *paramBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, paramShmemShift);

    PushActiveSnapshot(GetTransactionSnapshot());
    SerializedData response = MetaProcess(metaService, count, paramBuffer);
    PopActiveSnapshot();

    PERF_LATENCY_BEGIN(shmem_alloc, perf ? &perf->shmemAllocLatency : NULL);
    uint64_t responseShmemShift = FalconShmemAllocatorMalloc(allocator, response.size);
    if (responseShmemShift == 0)
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "FalconShmemAllocMalloc failed. Size: %u.", response.size);
    char *responseBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, responseShmemShift);
    FALCON_SHMEM_ALLOCATOR_SET_SIGNATURE(responseBuffer, signature);
    memcpy(responseBuffer, response.buffer, response.size);
    PERF_LATENCY_END(shmem_alloc);

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
    SerializedData response = MetaProcess(metaService, count, paramBuffer);
    PopActiveSnapshot();

    bytea *reply = (bytea *)palloc(VARHDRSZ + response.size);
    memcpy(VARDATA_4B(reply), response.buffer, response.size);
    SET_VARSIZE_4B(reply, VARHDRSZ + response.size);

    PG_RETURN_BYTEA_P(reply);
}
