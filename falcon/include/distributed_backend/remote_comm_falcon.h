/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_REMOTE_COMM_FALCON_H
#define FALCON_REMOTE_COMM_FALCON_H

#include "postgres.h"

#include "distributed_backend/remote_comm.h"
#include "lib/stringinfo.h"
#include "metadb/meta_handle.h"
#include "nodes/pg_list.h"

/* REMOTE_COMMAND_FLAG_BEGIN_TYPE_MASK controls whether we should send begin before send command to remote
 *
 * REMOTE_COMMAND_FLAG_NO_BEGIN:
 *      don't send begin
 * REMOTE_COMMAND_FLAG_NEED_TRANSACTION_SNAPSHOT:
 *      send begin, but 2pc is not required
 * REMOTE_COMMAND_FLAG_WRITE:
 *      send begin, and need 2pc
 *
 * REMOTE_COMMAND_FLAG_WRITE will override the effect of REMOTE_COMMAND_FLAG_NEED_TRANSACTION_SNAPSHOT
 */
#define REMOTE_COMMAND_FLAG_BEGIN_TYPE_MASK 0x3
#define REMOTE_COMMAND_FLAG_NO_BEGIN 0
#define REMOTE_COMMAND_FLAG_NEED_TRANSACTION_SNAPSHOT 1
#define REMOTE_COMMAND_FLAG_WRITE 2

#define REMOTE_COMMAND_FLAG_ALLOW_BATCH_WITH_OTHERS 4

void RemoteConnectionCommandCacheInit(void);

void ClearRemoteConnectionCommand(void);
void RegisterLocalProcessFlag(bool readOnly);
bool IsLocalWrite(void);

// Caller should make sure command contains only one sql query, otherwise it will be difficult
// to split result
int FalconPlainCommandOnWorkerList(const char *command, uint32_t remoteCommandFlag, List *workerIdList);
int FalconMetaCallOnWorkerList(FalconMetaServiceType metaService,
                               int32_t count,
                               SerializedData param,
                               uint32_t remoteCommandFlag,
                               List *workerIdList);

MultipleServerRemoteCommandResult FalconSendCommandAndWaitForResult(void);

// user is not expected to call these functions. They are called by transaction callbacks
void FalconRemoteCommandPrepare(void);
void FalconRemoteCommandCommit(void);
bool FalconRemoteCommandAbort(void);

#endif
