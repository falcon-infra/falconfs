/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "hcom_comm_adapter/falcon_meta_service_job.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include "hcom_comm_adapter/falcon_meta_service_internal.h"
#include "remote_connection_utils/error_code_def.h"
#include "log/logging.h"

namespace falcon {
namespace meta_service {

static void CleanupResponseData(FalconMetaServiceResponse &response)
{
    if (response.data == nullptr) {
        return;
    }
    switch (response.opcode) {
        case DFC_GET_KV_META:
            delete static_cast<KvDataResponse *>(response.data);
            break;
        case DFC_PLAIN_COMMAND:
            delete static_cast<PlainCommandResponse *>(response.data);
            break;
        case DFC_CREATE:
            delete static_cast<CreateResponse *>(response.data);
            break;
        case DFC_OPEN:
            delete static_cast<OpenResponse *>(response.data);
            break;
        case DFC_STAT:
            delete static_cast<StatResponse *>(response.data);
            break;
        case DFC_UNLINK:
            delete static_cast<UnlinkResponse *>(response.data);
            break;
        case DFC_READDIR:
            delete static_cast<ReadDirResponse *>(response.data);
            break;
        case DFC_OPENDIR:
            delete static_cast<OpenDirResponse *>(response.data);
            break;
        case DFC_RENAME_SUB_RENAME_LOCALLY:
            delete static_cast<RenameSubRenameLocallyResponse *>(response.data);
            break;
        case DFC_SLICE_GET:
            delete static_cast<SliceInfoResponse *>(response.data);
            break;
        case DFC_FETCH_SLICE_ID:
            delete static_cast<SliceIdResponse *>(response.data);
            break;
        default:
            break;
    }
    response.data = nullptr;
}

static FalconMetaServiceType ConvertOperationToServiceType(FalconMetaOperationType op)
{
    switch (op) {
        case DFC_PLAIN_COMMAND:
            return FalconMetaServiceType::PLAIN_COMMAND;
        case DFC_MKDIR:
            return FalconMetaServiceType::MKDIR;
        case DFC_MKDIR_SUB_MKDIR:
            return FalconMetaServiceType::MKDIR_SUB_MKDIR;
        case DFC_MKDIR_SUB_CREATE:
            return FalconMetaServiceType::MKDIR_SUB_CREATE;
        case DFC_CREATE:
            return FalconMetaServiceType::CREATE;
        case DFC_STAT:
            return FalconMetaServiceType::STAT;
        case DFC_OPEN:
            return FalconMetaServiceType::OPEN;
        case DFC_CLOSE:
            return FalconMetaServiceType::CLOSE;
        case DFC_UNLINK:
            return FalconMetaServiceType::UNLINK;
        case DFC_READDIR:
            return FalconMetaServiceType::READDIR;
        case DFC_OPENDIR:
            return FalconMetaServiceType::OPENDIR;
        case DFC_RMDIR:
            return FalconMetaServiceType::RMDIR;
        case DFC_RMDIR_SUB_RMDIR:
            return FalconMetaServiceType::RMDIR_SUB_RMDIR;
        case DFC_RMDIR_SUB_UNLINK:
            return FalconMetaServiceType::RMDIR_SUB_UNLINK;
        case DFC_RENAME:
            return FalconMetaServiceType::RENAME;
        case DFC_RENAME_SUB_RENAME_LOCALLY:
            return FalconMetaServiceType::RENAME_SUB_RENAME_LOCALLY;
        case DFC_RENAME_SUB_CREATE:
            return FalconMetaServiceType::RENAME_SUB_CREATE;
        case DFC_UTIMENS:
            return FalconMetaServiceType::UTIMENS;
        case DFC_CHOWN:
            return FalconMetaServiceType::CHOWN;
        case DFC_CHMOD:
            return FalconMetaServiceType::CHMOD;
        case DFC_PUT_KEY_META:
            return FalconMetaServiceType::KV_PUT;
        case DFC_GET_KV_META:
            return FalconMetaServiceType::KV_GET;
        case DFC_DELETE_KV_META:
            return FalconMetaServiceType::KV_DEL;
        case DFC_SLICE_PUT:
            return FalconMetaServiceType::SLICE_PUT;
        case DFC_SLICE_GET:
            return FalconMetaServiceType::SLICE_GET;
        case DFC_SLICE_DEL:
            return FalconMetaServiceType::SLICE_DEL;
        case DFC_FETCH_SLICE_ID:
            return FalconMetaServiceType::FETCH_SLICE_ID;
        default:
            return FalconMetaServiceType::NOT_SUPPORTED;
    }
}

static bool IsAllowBatchOperation(FalconMetaOperationType op)
{
    switch (op) {
        case DFC_MKDIR:
        case DFC_CREATE:
        case DFC_STAT:
        case DFC_OPEN:
        case DFC_CLOSE:
        case DFC_UNLINK:
            return true;
        default:
            return false;
    }
}

FalconMetaServiceJob::FalconMetaServiceJob(const FalconMetaServiceRequest &request,
                                       FalconMetaServiceCallback callback,
                                       void *user_context)
    : m_request(request),
      m_callback(callback),
      m_user_context(user_context)
{
    FalconErrorCode err = FalconMetaServiceSerializer::SerializeRequestToSerializedData(m_request, m_request_buffer);
    if (err != SUCCESS) {
        FALCON_LOG_PRINTF(LOG_WARNING,
                          "[FalconMetaService] Serialize request failed: opcode=%d, error=%d",
                          static_cast<int>(m_request.operation),
                          static_cast<int>(err));
        m_response.status = err;
    }
}

FalconMetaServiceJob::~FalconMetaServiceJob()
{
    CleanupResponseData(m_response);
}

void FalconMetaServiceJob::Done()
{
    LatencyData* e2eData = GetOpcodeE2ELatencyData(static_cast<int>(opcodeForE2E));
    e2eTimer.End(e2eData);

    if (m_callback) {
        m_callback(m_response, m_user_context);
        CleanupResponseData(m_response);
    }
}

bool FalconMetaServiceJob::IsAllowBatchProcess()
{
    return IsAllowBatchOperation(m_request.operation);
}

bool FalconMetaServiceJob::IsEmptyRequest()
{
    return false;
}

int FalconMetaServiceJob::GetReqServiceCnt()
{
    return 1;
}

size_t FalconMetaServiceJob::GetReqDatasize()
{
    return m_request_buffer.size();
}

size_t FalconMetaServiceJob::CopyOutData(void *dst, size_t dstSize)
{
    if (!dst || dstSize < m_request_buffer.size()) {
        return 0;
    }
    memcpy(dst, m_request_buffer.data(), m_request_buffer.size());
    return m_request_buffer.size();
}

FalconMetaServiceType FalconMetaServiceJob::GetFalconMetaServiceType(int index)
{
    if (index != 0) {
        FALCON_LOG_PRINTF(LOG_WARNING,
                          "[FalconMetaService] invalid service index: %d",
                          index);
        throw std::runtime_error("input index out of range.");
    }

    FalconMetaServiceType type = ConvertOperationToServiceType(m_request.operation);
    if (type == FalconMetaServiceType::NOT_SUPPORTED) {
        FALCON_LOG_PRINTF(LOG_WARNING,
                          "[FalconMetaService] unsupported service type: %d",
                          static_cast<int>(m_request.operation));
        throw std::runtime_error("got unsupported serviceType.");
    }
    return type;
}

void FalconMetaServiceJob::ProcessResponse(void *data, size_t size, FalDataDeleter deleter)
{
    m_response.opcode = m_request.operation;
    if (m_response.status != SUCCESS) {
        if (deleter) {
            deleter(data);
        } else if (data) {
            free(data);
        }
        return;
    }

    if (!FalconMetaServiceSerializer::DeserializeResponseFromSerializedData(data, size, &m_response, m_request.operation)) {
        FALCON_LOG_PRINTF(LOG_WARNING,
                          "[FalconMetaService] Failed to deserialize response for opcode=%d",
                          static_cast<int>(m_request.operation));
        m_response.status = -1;
    }

    if (deleter) {
        deleter(data);
    } else if (data) {
        free(data);
    }
}

} // namespace meta_service
} // namespace falcon
