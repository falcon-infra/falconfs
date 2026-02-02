/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */
#include "connection_pool/falcon_worker_task.h"
#include <sstream>
#include "falcon_meta_param_generated.h"
#include "falcon_meta_response_generated.h"
#include "perf_counter/perf_stat.h"
#include "remote_connection_utils/error_code_def.h"
#include "remote_connection_utils/serialized_data.h"

extern "C" {
#include "utils/error_code.h"
#include "utils/error_log.h"
#include "utils/utils_standalone.h"
}

void SingleWorkerTask::DoWork(PGconn *conn,
                              flatbuffers::FlatBufferBuilder &flatBufferBuilder,
                              SerializedData &replyBuilder)
{
    /* Report workerWaitLatency (final stage, no restart needed) */
    if (m_job != nullptr) {
        m_job->stageTimer.End(GetWorkerWaitLatencyData());
    }

    // 1. Reset status and check validity of input
    PGresult *res{nullptr};
    while ((res = PQgetResult(conn)) != NULL)
        PQclear(res);
    flatBufferBuilder.Clear();

    // this never should be happen, need make sure job not null while create SingleWorkerTask
    if (m_job == nullptr) {
        throw std::runtime_error("SingleWorkerTask: m_job is a nullptr");
    }
    // 2. Start processing
    // 2.1 Copy data into shmem
    size_t requestParamSize = m_job->GetReqDatasize();
    int requestServiceCount = m_job->GetReqServiceCnt();
    uint64_t sharedParamDataAddrShift = FalconShmemAllocatorMalloc(m_allocator, requestParamSize);
    if (sharedParamDataAddrShift == 0) {
        FALCON_ELOG_THREAD_SAFE_EXTENDED(
            "Shmem of connection pool is exhausted, requestParamSize: %zu. There may be "
            "several reasons, 1) shmem size is too small, 2) allocate too much memory "
            "once exceed FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE.",
            requestParamSize);
        throw std::runtime_error("memory exceed limit.");
    }
    char *paramBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(m_allocator, sharedParamDataAddrShift);
    m_job->CopyOutData(paramBuffer, requestParamSize);
    SerializedData requestData;
    if (!SerializedDataInit(&requestData, paramBuffer, requestParamSize, requestParamSize, NULL))
        throw std::runtime_error("request attachment is corrupt.");

    // 2.2 construct req msg
    std::stringstream toSendCommand;
    std::vector<bool> isPlainCommand;
    std::vector<int64_t> signatureList;
    int i = 0;
    uint64_t currentParamSegment = 0;
    while (i < requestServiceCount) {
        FalconMetaServiceType serviceType = m_job->GetFalconMetaServiceType(i);
        int j = i + 1;
        // merge same MetaServiceType into on request
        if (serviceType != FalconMetaServiceType::PLAIN_COMMAND) {
            while (j < requestServiceCount && m_job->GetFalconMetaServiceType(j) == serviceType)
                ++j;
        }
        int currentParamSegmentCount = j - i;
        uint32_t currentParamSegmentSize =
            SerializedDataNextSeveralItemSize(&requestData, currentParamSegment, currentParamSegmentCount);

        if (serviceType == FalconMetaServiceType::PLAIN_COMMAND) {
            // PLAIN_COMMAND just using the origin request content.
            char *buf = paramBuffer + currentParamSegment + SERIALIZED_DATA_ALIGNMENT;
            int size = currentParamSegmentSize - SERIALIZED_DATA_ALIGNMENT;
            flatbuffers::Verifier verifier((uint8_t *)buf, size);
            if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaParam>())
                throw std::runtime_error("request param is corrupt. 1");
            const falcon::meta_fbs::MetaParam *param = falcon::meta_fbs::GetMetaParam(buf);
            if (param->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_PlainCommandParam)
                throw std::runtime_error("request param is corrupt. 2");

            // split PGresult
            const char *command = param->param_as_PlainCommandParam()->command()->c_str();
            toSendCommand << command;
            isPlainCommand.push_back(true);
            signatureList.push_back(0);
        } else {
            // construct meta service request, meta service using
            signatureList.push_back(FalconShmemAllocatorGetUniqueSignature(m_allocator));
            toSendCommand << "select falcon_meta_call_by_serialized_shmem_internal(" << serviceType << ", "
                          << currentParamSegmentCount << ", " << sharedParamDataAddrShift + currentParamSegment << ", "
                          << signatureList.back() << ");";

            isPlainCommand.push_back(false);
        }

        currentParamSegment += currentParamSegmentSize;
        i = j;
    }

    // 2.3 Send request to PG worker process
    int sendQuerySucceed = PQsendQuery(conn, toSendCommand.str().c_str());
    if (sendQuerySucceed != static_cast<int>(isPlainCommand.size())) {
        throw std::runtime_error(PQerrorMessage(conn));
    }

    // 2.4 wait for process Result return
    std::vector<PGresult *> result;
    while ((res = PQgetResult(conn)) != NULL) {
        result.push_back(res);
    }

    FalconShmemAllocatorFree(m_allocator, sharedParamDataAddrShift);
    if (result.size() != isPlainCommand.size()) {
        throw std::runtime_error(
            "reply count cannot match request. maybe there is a request containing several plain commands.");
    }

    // 2.5 Process result
    SerializedData replyData;
    SerializedDataInit(&replyData, NULL, 0, 0, NULL);
    for (size_t i = 0; i < result.size(); ++i) {
        res = result[i];
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            char *totalErrorMsg = PQresultErrorMessage(res);
            const char *validErrorMsg = NULL;
            FalconErrorCode errorCode = FalconErrorMsgAnalyse(totalErrorMsg, &validErrorMsg);
            if (errorCode == SUCCESS)
                errorCode = PROGRAM_ERROR;

            flatBufferBuilder.Clear();
            auto metaResponse = falcon::meta_fbs::CreateMetaResponse(flatBufferBuilder, errorCode);
            flatBufferBuilder.Finish(metaResponse);

            char *buf = SerializedDataApplyForSegment(&replyData, flatBufferBuilder.GetSize());
            memcpy(buf, flatBufferBuilder.GetBufferPointer(), flatBufferBuilder.GetSize());
        } else if (isPlainCommand[i]) {
            flatBufferBuilder.Clear();
            std::vector<flatbuffers::Offset<flatbuffers::String>> plainCommandResponseData;
            int row = PQntuples(res);
            int col = PQnfields(res);
            for (int i = 0; i < row; ++i)
                for (int j = 0; j < col; ++j)
                    plainCommandResponseData.push_back(flatBufferBuilder.CreateString(PQgetvalue(res, i, j)));
            auto plainCommandResponse =
                falcon::meta_fbs::CreatePlainCommandResponse(flatBufferBuilder,
                                                             row,
                                                             col,
                                                             flatBufferBuilder.CreateVector(plainCommandResponseData));
            auto metaResponse = falcon::meta_fbs::CreateMetaResponse(
                flatBufferBuilder,
                SUCCESS,
                falcon::meta_fbs::AnyMetaResponse::AnyMetaResponse_PlainCommandResponse,
                plainCommandResponse.Union());
            flatBufferBuilder.Finish(metaResponse);

            char *buf = SerializedDataApplyForSegment(&replyData, flatBufferBuilder.GetSize());
            memcpy(buf, flatBufferBuilder.GetBufferPointer(), flatBufferBuilder.GetSize());
        } else {
            int64_t signature = signatureList[i];
            if (PQntuples(res) != 1 || PQnfields(res) != 1)
                throw std::runtime_error("returned reply is corrupt in non-batch operation. 1");
            uint64_t replyShift = (uint64_t)StringToInt64(PQgetvalue(res, 0, 0));
            char *replyBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(m_allocator, replyShift);
            if (FALCON_SHMEM_ALLOCATOR_GET_SIGNATURE(replyBuffer) != signature)
                throw std::runtime_error("returned reply is corrupt in non-batch operation. 2");
            uint64_t replyBufferSize = FALCON_SHMEM_ALLOCATOR_POINTER_GET_SIZE(replyBuffer);

            SerializedData oneReply;
            if (!SerializedDataInit(&oneReply, replyBuffer, replyBufferSize, replyBufferSize, NULL))
                throw std::runtime_error("reply data is corrupt.");
            SerializedDataAppend(&replyData, &oneReply);
            FalconShmemAllocatorFree(m_allocator, replyShift);
        }
    }

    // 2.5.1 SendResponse & recycle resource
    m_job->ProcessResponse(replyData.buffer, replyData.size, NULL);
    m_job->Done();

    for (size_t i = 0; i < result.size(); ++i) {
        PQclear(res);
    }

    delete m_job;
    m_job = nullptr;
}

void BatchWorkerTask::DoWork(PGconn *conn,
                             flatbuffers::FlatBufferBuilder &flatBufferBuilder,
                             SerializedData &replyBuilder)
{
    /* Report workerWaitLatency (final stage, no restart needed) for all jobs in batch */
    for (auto &job : m_jobList) {
        if (job != nullptr) {
            job->stageTimer.End(GetWorkerWaitLatencyData());
        }
    }

    // 1. Reset status and check validity of input
    PGresult *res{nullptr};
    while ((res = PQgetResult(conn)) != NULL)
        PQclear(res);
    flatBufferBuilder.Clear();

    // this never should be happen, need make sure jobList not empty while create BatchWorkerTask
    if (m_jobList.empty()) {
        throw std::runtime_error("BatchWorkerTask: jobList is empty");
    }

    // 2. Start processing
    // 2.1 Copy data into shmem
    // all ServiceType in one batch worker are same.
    FalconMetaServiceType serviceType = m_jobList[0]->GetFalconMetaServiceType(0);

    // calculate total totalRequestDataSize for allocate shared memory.
    uint32_t totalRequestServiceCount = 0;
    uint32_t totalRequestParamDataSize = 0;
    for (size_t i = 0; i < m_jobList.size(); ++i) {
        size_t reqDataSize = m_jobList[i]->GetReqDatasize();
        if ((reqDataSize & SERIALIZED_DATA_ALIGNMENT_MASK) != 0)
            throw std::runtime_error("param is corrupt."); // checked when init of job
        totalRequestServiceCount += m_jobList[i]->GetReqServiceCnt();
        totalRequestParamDataSize += reqDataSize;
    }

    // alloca shared memory for PQsendQuery
    int64_t signature = FalconShmemAllocatorGetUniqueSignature(m_allocator);
    uint64_t sharedParamDataAddrShift = FalconShmemAllocatorMalloc(m_allocator, totalRequestParamDataSize);
    if (sharedParamDataAddrShift == 0) {
        FALCON_ELOG_THREAD_SAFE_EXTENDED(
            "Shmem of connection pool is exhausted, totalParamSize: %u. There may be "
            "several reasons, 1) shmem size is too small, 2) allocate too much memory "
            "once exceed FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE.",
            totalRequestParamDataSize);
        throw std::runtime_error("memory exceed limit.");
    }

    // write RequestParamData&signature to shared memory
    uint64_t curStartOffset = sharedParamDataAddrShift;
    for (size_t i = 0; i < m_jobList.size(); ++i) {
        size_t curDataSize = m_jobList[i]->GetReqDatasize();
        m_jobList[i]->CopyOutData(FALCON_SHMEM_ALLOCATOR_GET_POINTER(m_allocator, curStartOffset), curDataSize);
        curStartOffset += curDataSize;
    }
    FALCON_SHMEM_ALLOCATOR_SET_SIGNATURE(FALCON_SHMEM_ALLOCATOR_GET_POINTER(m_allocator, sharedParamDataAddrShift),
                                         signature);

    // 2.2 construct req msg
    char command[128];
    sprintf(command,
            "select falcon_meta_call_by_serialized_shmem_internal(%d, %u, %ld, %ld);",
            serviceType,
            totalRequestServiceCount,
            (int64_t)sharedParamDataAddrShift,
            signature);

    // 2.3 Send request to PG worker process
    int sendQuerySucceed = PQsendQuery(conn, command);
    if (sendQuerySucceed != 1)
        throw std::runtime_error(PQerrorMessage(conn));

    // 2.4 wait for process Result return
    res = PQgetResult(conn);
    if (res == NULL)
        throw std::runtime_error(PQerrorMessage(conn));

    // now sharedParamData is useless, free the shared memory.
    FalconErrorCode errorCode = SUCCESS;
    FalconShmemAllocatorFree(m_allocator, sharedParamDataAddrShift);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        char *totalErrorMsg = PQresultErrorMessage(res);
        const char *validErrorMsg = NULL;
        errorCode = FalconErrorMsgAnalyse(totalErrorMsg, &validErrorMsg);
        if (errorCode == SUCCESS)
            errorCode = PROGRAM_ERROR;
    }

    // 2.5 Process result
    if (errorCode != SUCCESS) {
        SerializedDataClear(&replyBuilder);
        flatBufferBuilder.Clear();
        auto metaResponse = falcon::meta_fbs::CreateMetaResponse(flatBufferBuilder, errorCode);
        flatBufferBuilder.Finish(metaResponse);
        char *buf = SerializedDataApplyForSegment(&replyBuilder, flatBufferBuilder.GetSize());
        memcpy(buf, flatBufferBuilder.GetBufferPointer(), flatBufferBuilder.GetSize());

        for (size_t i = 0; i < m_jobList.size(); ++i) {
            char *data = (char *)malloc(replyBuilder.size);
            memcpy(data, replyBuilder.buffer, replyBuilder.size);
            // 2.5.1 SendResponse & clear resource
            m_jobList[i]->ProcessResponse(data, replyBuilder.size, NULL);
            m_jobList[i]->Done();
        }
    } else {
        if (PQntuples(res) != 1 || PQnfields(res) != 1) {
            throw std::runtime_error("returned reply is corrupt.");
        }
        uint64_t replyShift = 0;
        replyShift = (uint64_t)StringToInt64(PQgetvalue(res, 0, 0));
        if (replyShift != 0) {
            char *replyBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(m_allocator, replyShift);
            uint64_t replyBufferSize = FALCON_SHMEM_ALLOCATOR_POINTER_GET_SIZE(replyBuffer);
            SerializedData replyData;
            if (!SerializedDataInit(&replyData, replyBuffer, replyBufferSize, replyBufferSize, NULL))
                throw std::runtime_error("reply data is corrupt.");

            uint32_t p = 0;
            for (size_t i = 0; i < m_jobList.size(); ++i) {
                int count = m_jobList[i]->GetReqServiceCnt();
                uint32_t size = SerializedDataNextSeveralItemSize(&replyData, p, count);
                if (size == (sd_size_t)-1)
                    throw std::runtime_error("response is corrupt.");
                char *data = (char *)malloc(size);
                memcpy(data, replyBuffer + p, size);
                // 2.5.1 SendResponse & clear resource
                m_jobList[i]->ProcessResponse(data, size, NULL);
                m_jobList[i]->Done();
                p += size;
            }
            FalconShmemAllocatorFree(m_allocator, replyShift);
        } else {
            // 2.5.1 SendResponse & clear resource
            for (size_t i = 0; i < m_jobList.size(); ++i) {
                m_jobList[i]->Done();
            }
        }
    }

    // 2.6 recycle resource
    PQclear(res);
    for (size_t i = 0; i < m_jobList.size(); ++i) {
        delete m_jobList[i];
    }
    m_jobList.clear();
}
