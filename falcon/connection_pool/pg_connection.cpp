/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/pg_connection.h"

#include <iostream>
#include <sstream>

#include "falcon_meta_param_generated.h"
#include "falcon_meta_response_generated.h"

extern "C" {
#include "connection_pool/connection_pool.h"
#include "utils/error_code.h"
#include "utils/utils_standalone.h"
}

PGConnection::PGConnection(PGConnectionPool *parent, const char *ip, const int port, const char *userName)
{
    this->parent = parent;

    working = true;
    taskToExec = nullptr;

    std::stringstream ss;
    ss << "hostaddr=" << ip << " port=" << port << " user=" << userName << " dbname=postgres";
    conn = PQconnectdb(ss.str().c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        throw std::runtime_error(std::string("pg connection error: ") + PQerrorMessage(conn));
    }
    PGresult *res = PQexec(conn, "SELECT falcon_prepare_commands();");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        throw std::runtime_error(std::string("pg connection error: ") + PQresultErrorMessage(res));
    }

    SerializedDataInit(&replyBuilder, NULL, 0, 0, NULL);
    this->thread = std::thread(&PGConnection::BackgroundWorker, this);
}

void PGConnection::BackgroundWorker()
{
    while (working) {
        if (!working)
            break;
        this->tasksToExec.wait_dequeue(taskToExec);

        // 1. Reset status and check validity of input
        PGresult *res;
        while ((res = PQgetResult(conn)) != NULL)
            PQclear(res);
        flatBufferBuilder.Clear();

        if (taskToExec->jobList.size() == 0)
            throw std::runtime_error("pgconnection: taskToExec is empty");

        // 2. Start processing
        FalconErrorCode errorCode = SUCCESS;
        FalconShmemAllocator *allocator = &FalconConnectionPoolShmemAllocator;
        if (taskToExec->isBatch) {
            // 2.1.1
            // if is batch operation,
            falcon::meta_proto::MetaServiceType serviceType = taskToExec->jobList[0]->GetRequest()->type(0);

            uint32_t totalParamCount = 0;
            uint32_t totalParamSize = 0;
            for (size_t i = 0; i < taskToExec->jobList.size(); ++i) {
                size_t paramSize = taskToExec->jobList[i]->GetCntl()->request_attachment().size();
                if ((paramSize & SERIALIZED_DATA_ALIGNMENT_MASK) != 0)
                    throw std::runtime_error("param is corrupt."); // checked when init of job
                totalParamCount += taskToExec->jobList[i]->GetRequest()->type_size();
                totalParamSize += paramSize;
            }

            int64_t signature = FalconShmemAllocatorGetUniqueSignature(allocator);
            uint64_t totalParamShift = FalconShmemAllocatorMalloc(allocator, totalParamSize);
            if (totalParamShift == 0) {
                printf("Shmem of connection pool is exhausted, totalParamSize: %u. There may be "
                       "several reasons, 1) shmem size is too small, 2) allocate too much memory "
                       "once exceed FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE.",
                       totalParamSize);
                fflush(stdout);
                throw std::runtime_error("memory exceed limit.");
            }
            uint64_t p = totalParamShift;
            for (size_t i = 0; i < taskToExec->jobList.size(); ++i) {
                size_t paramSize = taskToExec->jobList[i]->GetCntl()->request_attachment().size();
                taskToExec->jobList[i]->GetCntl()->request_attachment().cutn(
                    FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, p),
                    paramSize);
                p += paramSize;
            }
            FALCON_SHMEM_ALLOCATOR_SET_SIGNATURE(FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, totalParamShift),
                                                 signature);

            // 2.1.2
            // barch operation can not be plain command
            uint64_t replyShift = 0;

            char command[128];
            sprintf(command,
                    "select falcon_meta_call_by_serialized_shmem_internal(%d, %u, %ld, %ld);",
                    serviceType,
                    totalParamCount,
                    (int64_t)totalParamShift,
                    signature);
            int sendQuerySucceed = PQsendQuery(conn, command);
            if (sendQuerySucceed != 1)
                throw std::runtime_error(PQerrorMessage(conn));

            PGresult *res = NULL;
            res = PQgetResult(conn);
            if (res == NULL)
                throw std::runtime_error(PQerrorMessage(conn));
            // param is useless now
            FalconShmemAllocatorFree(allocator, totalParamShift);
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                char *totalErrorMsg = PQresultErrorMessage(res);
                const char *validErrorMsg = NULL;
                errorCode = FalconErrorMsgAnalyse(totalErrorMsg, &validErrorMsg);
                if (errorCode == SUCCESS)
                    errorCode = PROGRAM_ERROR;
            }

            // 2.1.3 Process result
            if (errorCode != SUCCESS) {
                SerializedDataClear(&replyBuilder);
                flatBufferBuilder.Clear();
                auto metaResponse = falcon::meta_fbs::CreateMetaResponse(flatBufferBuilder, errorCode);
                flatBufferBuilder.Finish(metaResponse);
                char *buf = SerializedDataApplyForSegment(&replyBuilder, flatBufferBuilder.GetSize());
                memcpy(buf, flatBufferBuilder.GetBufferPointer(), flatBufferBuilder.GetSize());

                for (size_t i = 0; i < taskToExec->jobList.size(); ++i) {
                    brpc::Controller *cntl = taskToExec->jobList[i]->GetCntl();
                    char *data = (char *)malloc(replyBuilder.size);
                    memcpy(data, replyBuilder.buffer, replyBuilder.size);
                    cntl->response_attachment().append_user_data(data, replyBuilder.size, NULL);
                    taskToExec->jobList[i]->Done();
                }
            } else {
                if (PQntuples(res) != 1 || PQnfields(res) != 1) {
                    throw std::runtime_error("returned reply is corrupt.");
                }
                replyShift = (uint64_t)StringToInt64(PQgetvalue(res, 0, 0));
                if (replyShift != 0) {
                    char *replyBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, replyShift);
                    uint64_t replyBufferSize = FALCON_SHMEM_ALLOCATOR_POINTER_GET_SIZE(replyBuffer);
                    SerializedData replyData;
                    if (!SerializedDataInit(&replyData, replyBuffer, replyBufferSize, replyBufferSize, NULL))
                        throw std::runtime_error("reply data is corrupt.");

                    uint32_t p = 0;
                    for (size_t i = 0; i < taskToExec->jobList.size(); ++i) {
                        brpc::Controller *cntl = taskToExec->jobList[i]->GetCntl();

                        int count = taskToExec->jobList[i]->GetRequest()->type_size();
                        uint32_t size = SerializedDataNextSeveralItemSize(&replyData, p, count);
                        if (size == (sd_size_t)-1)
                            throw std::runtime_error("response is corrupt.");
                        char *data = (char *)malloc(size);
                        memcpy(data, replyBuffer + p, size);
                        cntl->response_attachment().append_user_data(data, size, NULL);

                        taskToExec->jobList[i]->Done();
                        p += size;
                    }
                    FalconShmemAllocatorFree(allocator, replyShift);
                } else {
                    for (size_t i = 0; i < taskToExec->jobList.size(); ++i) {
                        taskToExec->jobList[i]->Done();
                    }
                }
            }

            PQclear(res);
        } else {
            if (taskToExec->jobList.size() != 1)
                throw std::runtime_error("pgconnection: jobList.size() must be 1 for non-batch operation");

            // 2.2.1 Copy data into shmem
            falcon::meta_proto::AsyncMetaServiceJob *job = taskToExec->jobList[0];
            size_t paramSize = job->GetCntl()->request_attachment().size();
            uint64_t paramShift = FalconShmemAllocatorMalloc(allocator, paramSize);
            if (paramShift == 0) {
                printf("Shmem of connection pool is exhausted, paramSize: %zu. There may be "
                       "several reasons, 1) shmem size is too small, 2) allocate too much memory "
                       "once exceed FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE.",
                       paramSize);
                fflush(stdout);
                throw std::runtime_error("memory exceed limit.");
            }
            char *paramBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, paramShift);
            job->GetCntl()->request_attachment().cutn(paramBuffer, paramSize);
            SerializedData requestData;
            if (!SerializedDataInit(&requestData, paramBuffer, paramSize, paramSize, NULL))
                throw std::runtime_error("request attachment is corrupt.");

            // 2.2.2
            std::stringstream toSendCommand;
            std::vector<bool> isPlainCommand;
            std::vector<int64_t> signatureList;
            int i = 0;
            uint64_t currentParamSegment = 0;
            while (i < job->GetRequest()->type_size()) {
                falcon::meta_proto::MetaServiceType serviceType = job->GetRequest()->type(i);
                int j = i + 1;
                if (serviceType != falcon::meta_proto::MetaServiceType::PLAIN_COMMAND) {
                    while (j < job->GetRequest()->type_size() && job->GetRequest()->type(j) == serviceType)
                        ++j;
                }
                int currentParamSegmentCount = j - i;

                uint32_t currentParamSegmentSize =
                    SerializedDataNextSeveralItemSize(&requestData, currentParamSegment, j - i);

                if (serviceType == falcon::meta_proto::MetaServiceType::PLAIN_COMMAND) {
                    //
                    char *buf = paramBuffer + currentParamSegment + SERIALIZED_DATA_ALIGNMENT;
                    int size = currentParamSegmentSize - SERIALIZED_DATA_ALIGNMENT;
                    flatbuffers::Verifier verifier((uint8_t *)buf, size);
                    if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaParam>())
                        throw std::runtime_error("request param is corrupt. 1");
                    const falcon::meta_fbs::MetaParam *param = falcon::meta_fbs::GetMetaParam(buf);
                    if (param->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_PlainCommandParam)
                        throw std::runtime_error("request param is corrupt. 2");

                    //
                    // split PGresult
                    const char *command = param->param_as_PlainCommandParam()->command()->c_str();

                    toSendCommand << command;

                    isPlainCommand.push_back(true);
                    signatureList.push_back(0);
                } else {
                    signatureList.push_back(FalconShmemAllocatorGetUniqueSignature(allocator));
                    toSendCommand << "select falcon_meta_call_by_serialized_shmem_internal(" << serviceType << ", "
                                  << currentParamSegmentCount << ", " << paramShift + currentParamSegment << ", "
                                  << signatureList.back() << ");";

                    isPlainCommand.push_back(false);
                }

                currentParamSegment += currentParamSegmentSize;
                i = j;
            }

            // 2.2.3
            PQsendQuery(conn, toSendCommand.str().c_str());
            std::vector<PGresult *> result;
            PGresult *res = NULL;
            while ((res = PQgetResult(conn)) != NULL)
                result.push_back(res);
            FalconShmemAllocatorFree(allocator, paramShift);
            if (result.size() != isPlainCommand.size()) {
                throw std::runtime_error(
                    "reply count cannot match request. maybe there is a request containing several plain commands.");
            }
            // 2.2.4
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
                    auto plainCommandResponse = falcon::meta_fbs::CreatePlainCommandResponse(
                        flatBufferBuilder,
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
                    char *replyBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, replyShift);
                    if (FALCON_SHMEM_ALLOCATOR_GET_SIGNATURE(replyBuffer) != signature)
                        throw std::runtime_error("returned reply is corrupt in non-batch operation. 2");
                    uint64_t replyBufferSize = FALCON_SHMEM_ALLOCATOR_POINTER_GET_SIZE(replyBuffer);

                    SerializedData oneReply;
                    if (!SerializedDataInit(&oneReply, replyBuffer, replyBufferSize, replyBufferSize, NULL))
                        throw std::runtime_error("reply data is corrupt.");
                    SerializedDataAppend(&replyData, &oneReply);
                    FalconShmemAllocatorFree(allocator, replyShift);
                }
            }
            //
            //
            // SerializedDataDestroy
            job->GetCntl()->response_attachment().append_user_data(replyData.buffer, replyData.size, NULL);
            job->Done();

            for (size_t i = 0; i < result.size(); ++i)
                PQclear(res);
        }

        // TBD
        //
        //

        this->parent->ReaddWorkingPGConnection(this);
        for (size_t i = 0; i < taskToExec->jobList.size(); ++i)
            delete taskToExec->jobList[i];
        
        this->taskToExec = nullptr;
    }
}

void PGConnection::Exec(std::shared_ptr<WorkerTask> taskToExec)
{
    while (!this->tasksToExec.enqueue(taskToExec)) {
        std::cout << "PGConnection::Exec: enqueue failed" << std::endl;
        std::this_thread::yield();
    }
}

void PGConnection::Stop()
{
    working = false;
}

PGConnection::~PGConnection()
{
    Stop();
    thread.join();
    if (conn) {
        PQfinish(conn);
        conn = nullptr;
    }
    SerializedDataDestroy(&replyBuilder);
}
