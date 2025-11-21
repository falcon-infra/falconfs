/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "distributed_backend/remote_comm_falcon.h"

#include <arpa/inet.h>

#include "access/xact.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "utils/memutils.h"

#include "distributed_backend/remote_comm.h"
#include "metadb/foreign_server.h"
#include "metadb/meta_handle.h"
#include "metadb/meta_serialize_interface_helper.h"
#include "metadb/shard_table.h"
#include "transaction/transaction.h"
#include "transaction/transaction_cleanup.h"
#include "utils/error_log.h"

typedef struct RemoteCommand
{
    FalconSupportMetaService metaService;
    int count;
    union
    {
        const char *plainCommand;
        SerializedData param;
    } data;
} RemoteCommand;
typedef struct RemoteConnectionCommandData
{
    int32_t serverId;
    uint32_t commandFlag;

    List *remoteCommandList;
} RemoteConnectionCommandData;
static MemoryContext RemoteConnectionCommandCacheContext = NULL;
static HTAB *RemoteConnectionCommandCache = NULL; // clear after eacg command ends

static List *PGresultToBeCleared = NIL;
static void inline MarkPGresultToBeClearedLater(PGresult *res)
{
    if (res == NULL)
        return;
    MemoryContext oldContext = MemoryContextSwitchTo(RemoteConnectionCommandCacheContext);
    PGresultToBeCleared = lappend(PGresultToBeCleared, res);
    MemoryContextSwitchTo(oldContext);
}

static bool LocalServerWrite = false;

void ClearRemoteConnectionCommand()
{
    if (PGresultToBeCleared) {
        for (int i = 0; i < list_length(PGresultToBeCleared); ++i) {
            PGresult *res = list_nth(PGresultToBeCleared, i);
            PQclear(res);
        }
        PGresultToBeCleared = NIL;
    }
    if (RemoteConnectionCommandCache != NULL) {
        MemoryContextReset(RemoteConnectionCommandCacheContext);
        RemoteConnectionCommandCache = NULL;
    }
    LocalServerWrite = false;
    RemoteTransactionGid[0] = '\0';
}

void RegisterLocalProcessFlag(bool readOnly)
{
    if (!readOnly) {
        LocalServerWrite = true;
    }
}

bool IsLocalWrite() { return LocalServerWrite; }

static int
FalconAppendRemoteCommandToWorkerList(RemoteCommand *remoteCommand, uint32_t remoteCommandFlag, List *workerIdList)
{
    if (RemoteConnectionCommandCache == NULL)
        RemoteConnectionCommandCacheInit();

    for (int i = 0; i < list_length(workerIdList); ++i) {
        int32_t serverId = list_nth_int(workerIdList, i);

        bool found;
        RemoteConnectionCommandData *remoteConnectionCommandData =
            hash_search(RemoteConnectionCommandCache, &serverId, HASH_ENTER, &found);
        if (!found) {
            remoteConnectionCommandData->serverId = serverId;
            remoteConnectionCommandData->commandFlag = 0;
            remoteConnectionCommandData->remoteCommandList = NIL;
        }

        remoteConnectionCommandData->commandFlag |= remoteCommandFlag;
        MemoryContext oldContext = MemoryContextSwitchTo(RemoteConnectionCommandCacheContext);
        remoteConnectionCommandData->remoteCommandList =
            lappend(remoteConnectionCommandData->remoteCommandList, remoteCommand);
        MemoryContextSwitchTo(oldContext);
    }
    return 0;
}

int FalconPlainCommandOnWorkerList(const char *command, uint32_t remoteCommandFlag, List *workerIdList)
{
    if (RemoteConnectionCommandCache == NULL)
        RemoteConnectionCommandCacheInit();

    MemoryContext oldContext = MemoryContextSwitchTo(RemoteConnectionCommandCacheContext);
    RemoteCommand *remoteCommand = palloc(sizeof(RemoteCommand));
    remoteCommand->metaService = PLAIN_COMMAND;
    remoteCommand->count = 1;
    remoteCommand->data.plainCommand = command;
    MemoryContextSwitchTo(oldContext);

    return FalconAppendRemoteCommandToWorkerList(remoteCommand, remoteCommandFlag, workerIdList);
}

int FalconMetaCallOnWorkerList(FalconSupportMetaService metaService,
                               int32_t count,
                               SerializedData param,
                               uint32_t remoteCommandFlag,
                               List *workerIdList)
{
    if (metaService == PLAIN_COMMAND)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "Use FalconPlainCommandOnWorkerList instead.");

    if (RemoteConnectionCommandCache == NULL)
        RemoteConnectionCommandCacheInit();

    MemoryContext oldContext = MemoryContextSwitchTo(RemoteConnectionCommandCacheContext);
    RemoteCommand *remoteCommand = palloc(sizeof(RemoteCommand));
    remoteCommand->metaService = metaService;
    remoteCommand->count = count;
    remoteCommand->data.param = param;
    MemoryContextSwitchTo(oldContext);

    return FalconAppendRemoteCommandToWorkerList(remoteCommand, remoteCommandFlag, workerIdList);
}

static inline PGresult *FetchPGresultAndMark(PGconn *conn)
{
    PGresult *res = PQgetResult(conn);
    if (res != NULL)
        MarkPGresultToBeClearedLater(res);
    return res;
}

static bool CheckPQpipelineSyncFinished(PGconn *conn)
{
    PGresult *res = NULL;
    res = FetchPGresultAndMark(conn);
    if (PQresultStatus(res) != PGRES_PIPELINE_SYNC)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "Unexpected result. There must be a PGRES_PIPELINE_SYNC.");
    res = FetchPGresultAndMark(conn);
    if (res != NULL)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "Unexpected result. There must be a NULL.");
    return true;
}

static bool inline ClearPGresultInPGconn(PGconn *conn)
{
    PGresult *res;
    bool hasUnfetchedPGresult = false;
    // clear result if ...
    while (true) {
        // only two continues NULL result implies the conn is cleared
        res = FetchPGresultAndMark(conn);
        if (res != NULL) {
            hasUnfetchedPGresult = true;
            continue;
        }
        res = FetchPGresultAndMark(conn);
        if (res != NULL) {
            hasUnfetchedPGresult = true;
            continue;
        }
        break;
    }
    return hasUnfetchedPGresult;
}

MultipleServerRemoteCommandResult FalconSendCommandAndWaitForResult()
{
    List *workerIdList = NIL;
    List *remoteConnectionCommandDataList = NIL;
    HASH_SEQ_STATUS status;
    RemoteConnectionCommandData *entry;
    hash_seq_init(&status, RemoteConnectionCommandCache);
    while ((entry = hash_seq_search(&status)) != 0) {
        if (list_length(entry->remoteCommandList) == 0) // this means this command is sent before
            continue;
        workerIdList = lappend_int(workerIdList, entry->serverId);
        remoteConnectionCommandDataList = lappend(remoteConnectionCommandDataList, entry);
    }

    List *connList = GetForeignServerConnection(workerIdList);

    PGresult *res;
    List *hasTransactionControlSent = NIL;
    List *remoteCommandListSent = NIL;
    for (int i = 0; i < list_length(connList); ++i) {
        ForeignServerConnection *foreignServerConn = list_nth(connList, i);
        RemoteConnectionCommandData *remoteConnectionCommand = list_nth(remoteConnectionCommandDataList, i);

        ClearPGresultInPGconn(foreignServerConn->conn);

        if (foreignServerConn->transactionState == FALCON_REMOTE_TRANSACTION_NONE &&
            (remoteConnectionCommand->commandFlag & REMOTE_COMMAND_FLAG_BEGIN_TYPE_MASK) !=
                REMOTE_COMMAND_FLAG_NO_BEGIN) {
            if (!PQsendQueryParams(foreignServerConn->conn, "BEGIN;", 0, NULL, NULL, NULL, NULL, 0))
                FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED,
                                           "error while tring to send 'BEGIN;', "
                                           "workerId: %d, errMsg: %s.",
                                           foreignServerConn->serverId,
                                           PQerrorMessage(foreignServerConn->conn));

            hasTransactionControlSent = lappend_int(hasTransactionControlSent, 1);
        } else {
            hasTransactionControlSent = lappend_int(hasTransactionControlSent, 0);
        }

        for (int j = 0; j < list_length(remoteConnectionCommand->remoteCommandList); ++j) {
            RemoteCommand *remoteCommand = list_nth(remoteConnectionCommand->remoteCommandList, j);

            if (remoteCommand->metaService == PLAIN_COMMAND) {
                if (!PQsendQueryParams(foreignServerConn->conn,
                                       remoteCommand->data.plainCommand,
                                       0,
                                       NULL,
                                       NULL,
                                       NULL,
                                       NULL,
                                       0))
                    FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED,
                                               "error while tring to send query "
                                               "'%s', workerId: %d, errMsg: %s.",
                                               remoteCommand->data.plainCommand,
                                               foreignServerConn->serverId,
                                               PQerrorMessage(foreignServerConn->conn));
            } else {
                int32_t type = htonl(MetaServiceTypeEncode(remoteCommand->metaService));
                int32_t count = htonl(remoteCommand->count);
                const char *const paramValues[3] = {(char *)&type, (char *)&count, remoteCommand->data.param.buffer};
                const int paramLengths[3] = {sizeof(int32_t), sizeof(int32_t), remoteCommand->data.param.size};
                const int paramFormats[3] = {1, 1, 1};
                if (!PQsendQueryPrepared(foreignServerConn->conn,
                                         "cs_meta_call",
                                         3,
                                         paramValues,
                                         paramLengths,
                                         paramFormats,
                                         1))
                    FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED,
                                               "error while tring to send meta "
                                               "call: service type = %d, workerId: %d, errMsg: %s.",
                                               type,
                                               foreignServerConn->serverId,
                                               PQerrorMessage(foreignServerConn->conn));
            }
        }
        if (!PQpipelineSync(foreignServerConn->conn))
            FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED,
                                       "error while tring to call PQpipelineSync, "
                                       "workerId: %d, errMsg: %s.",
                                       foreignServerConn->serverId,
                                       PQerrorMessage(foreignServerConn->conn));

        if (foreignServerConn->transactionState == FALCON_REMOTE_TRANSACTION_NONE) {
            if (remoteConnectionCommand->commandFlag & REMOTE_COMMAND_FLAG_WRITE) {
                foreignServerConn->transactionState = FALCON_REMOTE_TRANSACTION_BEGIN_FOR_WRITE;
            } else if (remoteConnectionCommand->commandFlag & REMOTE_COMMAND_FLAG_NEED_TRANSACTION_SNAPSHOT) {
                foreignServerConn->transactionState = FALCON_REMOTE_TRANSACTION_BEGIN_FOR_SNAPSHOT;
            }
        }

        remoteCommandListSent = lappend(remoteCommandListSent, remoteConnectionCommand->remoteCommandList);
        remoteConnectionCommand->remoteCommandList = NIL;
    }

    MultipleServerRemoteCommandResult multipleServerRemoteCommandResult = NIL;
    for (int i = 0; i < list_length(connList); ++i) {
        RemoteCommandResultPerServerData *resPerServer = palloc(sizeof(RemoteCommandResultPerServerData));
        resPerServer->serverId = list_nth_int(workerIdList, i);
        resPerServer->remoteCommandResult = NIL; //
        ForeignServerConnection *foreignServerConn = list_nth(connList, i);
        bool needHandleTransactionControl = list_nth_int(hasTransactionControlSent, i);

        if (needHandleTransactionControl) {
            res = FetchPGresultAndMark(foreignServerConn->conn);
            if (PQresultStatus(res) != PGRES_COMMAND_OK)
                FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED,
                                           "workerId: %d, errorMsg: %s, PQresultStatus: %d.",
                                           foreignServerConn->serverId,
                                           PQresultErrorMessage(res),
                                           PQresultStatus(res));

            res = FetchPGresultAndMark(foreignServerConn->conn);
            if (res != NULL)
                FALCON_ELOG_ERROR(PROGRAM_ERROR, "a NULL is expected");
        }

        bool done = false;
        while (!done) {
            res = FetchPGresultAndMark(foreignServerConn->conn);
            switch (PQresultStatus(res)) {
            case PGRES_COMMAND_OK:
            case PGRES_TUPLES_OK:
                resPerServer->remoteCommandResult = lappend(resPerServer->remoteCommandResult, res);
                break;
            case PGRES_PIPELINE_SYNC:
                done = true;
                break;
            default:
                FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED,
                                           "workerId: %d, errorMsg: %s.",
                                           foreignServerConn->serverId,
                                           PQresultErrorMessage(res));
            }

            res = FetchPGresultAndMark(foreignServerConn->conn);
            if (res != NULL)
                FALCON_ELOG_ERROR(PROGRAM_ERROR, "a NULL is expected.");
        }

        multipleServerRemoteCommandResult = lappend(multipleServerRemoteCommandResult, resPerServer);
    }

    return multipleServerRemoteCommandResult;
}

void FalconRemoteCommandPrepare()
{
    if (RemoteConnectionCommandCache == NULL) // no command sent
        return;

    List *workerIdList = NIL;
    HASH_SEQ_STATUS status;
    RemoteConnectionCommandData *entry;
    hash_seq_init(&status, RemoteConnectionCommandCache);
    while ((entry = hash_seq_search(&status)) != 0) {
        workerIdList = lappend_int(workerIdList, entry->serverId);
    }
    List *connList = GetForeignServerConnection(workerIdList);

    int writeServerCount = LocalServerWrite ? 1 : 0;
    for (int i = 0; i < list_length(workerIdList); ++i) {
        ForeignServerConnection *foreignServerConn = list_nth(connList, i);

        if (foreignServerConn->transactionState == FALCON_REMOTE_TRANSACTION_BEGIN_FOR_WRITE)
            ++writeServerCount;
    }
    bool need2pc = (writeServerCount >= 2);
    if (!need2pc) {
        // do nothing if don't need 2pc
        return;
    }

    strcpy(RemoteTransactionGid, GetImplicitTransactionGid()->data);
    AddInprogressTransaction(RemoteTransactionGid); // for 2pc cleanup

    char prepareCommand[MAX_TRANSACTION_GID_LENGTH + 24];
    sprintf(prepareCommand, "PREPARE TRANSACTION '%s';", RemoteTransactionGid);
    for (int i = 0; i < list_length(connList); ++i) {
        ForeignServerConnection *foreignServerConn = list_nth(connList, i);
        if (ClearPGresultInPGconn(foreignServerConn->conn))
            FALCON_ELOG_ERROR(PROGRAM_ERROR,
                              "Has unfetched PGresult when trying to send prepare. "
                              "There must be something wrong.");

        if (foreignServerConn->transactionState == FALCON_REMOTE_TRANSACTION_BEGIN_FOR_WRITE) {
            if (!PQsendQueryParams(foreignServerConn->conn, prepareCommand, 0, NULL, NULL, NULL, NULL, 0))
                FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED,
                                           "error while trying to send prepare command '%s', workerId: %d, errMsg: %s.",
                                           prepareCommand,
                                           foreignServerConn->serverId,
                                           PQerrorMessage(foreignServerConn->conn));

            if (!PQpipelineSync(foreignServerConn->conn))
                FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "error while trying to sync pipeline.");
        }
    }

    StringInfo errorMsg = makeStringInfo();
    for (int i = 0; i < list_length(workerIdList); ++i) {
        int32_t serverId = list_nth_int(workerIdList, i);
        ForeignServerConnection *foreignServerConn = list_nth(connList, i);

        PGresult *res = FetchPGresultAndMark(foreignServerConn->conn);
        if (res == NULL)
            continue;

        switch (PQresultStatus(res)) {
        case PGRES_COMMAND_OK:
        case PGRES_TUPLES_OK:
            res = FetchPGresultAndMark(foreignServerConn->conn);
            if (res != NULL)
                FALCON_ELOG_ERROR(PROGRAM_ERROR, "a NULL is expected.");

            CheckPQpipelineSyncFinished(foreignServerConn->conn);

            if (foreignServerConn->transactionState == FALCON_REMOTE_TRANSACTION_BEGIN_FOR_WRITE) {
                foreignServerConn->transactionState = FALCON_REMOTE_TRANSACTION_PREPARE;
                Write2PCRecord(serverId, RemoteTransactionGid); // for 2pc cleanup
            }
            break;
        default:
            appendStringInfo(errorMsg,
                             "workerId: %d, errorMsg: %s;",
                             foreignServerConn->serverId,
                             PQresultErrorMessage(res));
        }
    }

    if (errorMsg->len != 0)
        FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED, "%s", errorMsg->data);
}

void FalconRemoteCommandCommit()
{
    if (RemoteConnectionCommandCache == NULL) // no command sent
        return;

    List *workerIdList = NIL;
    HASH_SEQ_STATUS status;
    RemoteConnectionCommandData *entry;
    hash_seq_init(&status, RemoteConnectionCommandCache);
    while ((entry = hash_seq_search(&status)) != 0) {
        workerIdList = lappend_int(workerIdList, entry->serverId);
    }
    List *connList = GetForeignServerConnection(workerIdList);

    char commitPreparedCommand[MAX_TRANSACTION_GID_LENGTH + 20];
    if (RemoteTransactionGid[0] != '\0') // 2pc is necessary
    {
        sprintf(commitPreparedCommand, "COMMIT PREPARED '%s';", RemoteTransactionGid);
    }
    for (int i = 0; i < list_length(workerIdList); ++i) {
        ForeignServerConnection *foreignServerConn = list_nth(connList, i);
        if (ClearPGresultInPGconn(foreignServerConn->conn)) {
            FALCON_ELOG_WARNING(PROGRAM_ERROR,
                                "Has unfetched PGresult when trying to send commit. "
                                "There must be something wrong.");
            return;
        }

        switch (foreignServerConn->transactionState) {
        case FALCON_REMOTE_TRANSACTION_BEGIN_FOR_SNAPSHOT:
        case FALCON_REMOTE_TRANSACTION_BEGIN_FOR_WRITE:
            if (!PQsendQueryParams(foreignServerConn->conn, "COMMIT;", 0, NULL, NULL, NULL, NULL, 0)) {
                FALCON_ELOG_WARNING_EXTENDED(
                    REMOTE_QUERY_FAILED,
                    "error while trying to send commit command 'COMMIT;', workerId: %d, errMsg: %s.",
                    foreignServerConn->serverId,
                    PQerrorMessage(foreignServerConn->conn));
                return;
            }
            if (!PQpipelineSync(foreignServerConn->conn)) {
                FALCON_ELOG_WARNING(REMOTE_QUERY_FAILED, "error while trying to sync pipeline.");
                return;
            }
            break;
        case FALCON_REMOTE_TRANSACTION_PREPARE:
            if (!PQsendQueryParams(foreignServerConn->conn, commitPreparedCommand, 0, NULL, NULL, NULL, NULL, 0)) {
                FALCON_ELOG_WARNING_EXTENDED(
                    REMOTE_QUERY_FAILED,
                    "error while trying to send commit command '%s', workerId: %d, errMsg: %s.",
                    commitPreparedCommand,
                    foreignServerConn->serverId,
                    PQerrorMessage(foreignServerConn->conn));
                return;
            }
            if (!PQpipelineSync(foreignServerConn->conn)) {
                FALCON_ELOG_WARNING(REMOTE_QUERY_FAILED, "error while trying to sync pipeline.");
                return;
            }
            break;
        default:
            // otherwise, do nothing
            break;
        }
    }

    for (int i = 0; i < list_length(connList); ++i) {
        ForeignServerConnection *foreignServerConn = list_nth(connList, i);

        PGresult *res = FetchPGresultAndMark(foreignServerConn->conn);
        if (res == NULL)
            continue;

        switch (PQresultStatus(res)) {
        case PGRES_COMMAND_OK:
        case PGRES_TUPLES_OK:
            res = FetchPGresultAndMark(foreignServerConn->conn);
            if (res != NULL) {
                FALCON_ELOG_WARNING(PROGRAM_ERROR, "a NULL is expected.");
                return;
            }

            CheckPQpipelineSyncFinished(foreignServerConn->conn);
            break;
        default:
            FALCON_ELOG_WARNING_EXTENDED(REMOTE_QUERY_FAILED,
                                         "workerId: %d, errorMsg: %s.",
                                         foreignServerConn->serverId,
                                         PQresultErrorMessage(res));
            return;
        }

        foreignServerConn->transactionState = FALCON_REMOTE_TRANSACTION_NONE;
    }
}

bool FalconRemoteCommandAbort()
{
    if (RemoteConnectionCommandCache == NULL) // no command sent
        return true;

    List *workerIdList = NIL;
    HASH_SEQ_STATUS status;
    RemoteConnectionCommandData *entry;
    hash_seq_init(&status, RemoteConnectionCommandCache);
    while ((entry = hash_seq_search(&status)) != 0) {
        workerIdList = lappend_int(workerIdList, entry->serverId);
    }
    List *connList = GetForeignServerConnection(workerIdList);

    char rollbackPreparedCommand[MAX_TRANSACTION_GID_LENGTH + 22];
    if (RemoteTransactionGid[0] != '\0') // 2pc is necessary
    {
        sprintf(rollbackPreparedCommand, "ROLLBACK PREPARED '%s';", RemoteTransactionGid);
    }
    bool succeed = true;

    for (int i = 0; i < list_length(workerIdList); ++i) {
        ForeignServerConnection *foreignServerConn = list_nth(connList, i);
        if (foreignServerConn->conn == NULL) {
            // skip failed connection
            succeed = false;
            continue;
        }

        ClearPGresultInPGconn(foreignServerConn->conn);

        int succeedThisTime = false;
        switch (foreignServerConn->transactionState) {
        case FALCON_REMOTE_TRANSACTION_BEGIN_FOR_SNAPSHOT:
        case FALCON_REMOTE_TRANSACTION_BEGIN_FOR_WRITE:
            if (!PQsendQueryParams(foreignServerConn->conn, "ROLLBACK;", 0, NULL, NULL, NULL, NULL, 0))
                break;
            if (!PQpipelineSync(foreignServerConn->conn))
                break;
            succeedThisTime = true;
            break;
        case FALCON_REMOTE_TRANSACTION_PREPARE:
            if (!PQsendQueryParams(foreignServerConn->conn, rollbackPreparedCommand, 0, NULL, NULL, NULL, NULL, 0))
                break;
            if (!PQpipelineSync(foreignServerConn->conn))
                break;
            succeedThisTime = true;
            break;
        default:
            // otherwise, do nothing
            succeedThisTime = true;
            break;
        }

        if (!succeedThisTime)
            succeed = false;
    }

    for (int i = 0; i < list_length(connList); ++i) {
        ForeignServerConnection *foreignServerConn = list_nth(connList, i);
        if (foreignServerConn->conn == NULL) {
            continue;
        }

        PGresult *res = FetchPGresultAndMark(foreignServerConn->conn);
        while (res != NULL) {
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                succeed = false;
                break;
            }

            res = FetchPGresultAndMark(foreignServerConn->conn);
            if (res != NULL) {
                succeed = false;
                break;
            }

            res = FetchPGresultAndMark(foreignServerConn->conn);
            if (PQresultStatus(res) != PGRES_PIPELINE_SYNC) {
                succeed = false;
                break;
            }

            res = FetchPGresultAndMark(foreignServerConn->conn);
            if (res != NULL) {
                succeed = false;
                break;
            }

            break;
        }

        foreignServerConn->transactionState = FALCON_REMOTE_TRANSACTION_NONE;
    }
    return succeed;
}

void RemoteConnectionCommandCacheInit()
{
    if (RemoteConnectionCommandCacheContext == NULL) {
        RemoteConnectionCommandCacheContext =
            AllocSetContextCreateInternal(TopMemoryContext,
                                          "Falcon Remote Connection Command Cache Context",
                                          ALLOCSET_DEFAULT_MINSIZE,
                                          ALLOCSET_DEFAULT_INITSIZE,
                                          ALLOCSET_DEFAULT_MAXSIZE);
    }

    HASHCTL info;
    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(int32_t);
    info.entrysize = sizeof(RemoteConnectionCommandData);
    info.hcxt = RemoteConnectionCommandCacheContext;
    int hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
    RemoteConnectionCommandCache =
        hash_create("Falcon Remote Comm Command Cache Hash Table", FOREIGN_SERVER_NUM_EXPECT, &info, hashFlags);
}
