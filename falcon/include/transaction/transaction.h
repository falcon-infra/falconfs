/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_TRANSACTION_H
#define FALCON_TRANSACTION_H

#include "postgres.h"

#include "lib/stringinfo.h"

// implicit2PC_head
#define FALCON_TRANSACTION_2PC_HEAD "falcon_xact_"
#define MAX_TRANSACTION_GID_LENGTH 127
extern char
    PreparedTransactionGid[MAX_TRANSACTION_GID_LENGTH + 1];       // saved gid for PREPARE TRANSACTION & COMMIT PREPARED
extern char RemoteTransactionGid[MAX_TRANSACTION_GID_LENGTH + 1]; // saved gid for remote 2pc

// explicit transaction state
// use by meta_func
typedef enum FalconExplicitTransactionState {
    FALCON_EXPLICIT_TRANSACTION_NONE,
    FALCON_EXPLICIT_TRANSACTION_BEGIN,
    FALCON_EXPLICIT_TRANSACTION_PREPARED
} FalconExplicitTransactionState;

// remote transaction state
typedef enum FalconRemoteTransactionState {
    FALCON_REMOTE_TRANSACTION_NONE,
    FALCON_REMOTE_TRANSACTION_BEGIN_FOR_WRITE,
    FALCON_REMOTE_TRANSACTION_BEGIN_FOR_SNAPSHOT,
    FALCON_REMOTE_TRANSACTION_PREPARE
} FalconRemoteTransactionState;

// same as begin. the caller should call FalconExplicitTransactionCommit to commit or rollback explicitly.
// however, if the transaction is aborted, we will call FalconExpllcitTransnctionRollback
// to rollback automatically.
extern void FalconExplicitTransactionBegin(void);
extern bool FalconExplicitTransactionCommit(void);
extern void FalconExplicitTransactionRollback(void);
extern bool FalconExplicitTransactionPrepare(const char *gid);
extern void FalconExplicitTransactionCommitPrepared(const char *gid);
extern void FalconExplicitTransactionRollbackPrepared(const char *gid);

void RegisterFalconTransactionCallback(void);

void TransactionManagerInit(void);

StringInfo GetImplicitTransactionGid(void);

#endif
