/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef TRANSACTION_CLEANUP_H
#define TRANSACTION_CLEANUP_H

#include "postgres.h"

#include "transaction/transaction.h"

typedef struct InprogressTransactionData
{
    char gid[MAX_TRANSACTION_GID_LENGTH];
} InprogressTransactionData;

#define MAX_INPROGRESS_TRANSACTION_COUNT 1024

extern int Recover2PCIntervalTime;

extern size_t TransactionCleanupShmemsize(void);
extern void TransactionCleanupShmemInit(void);

extern void AddInprogressTransaction(const char *transactionName);
extern void RemoveInprogressTransaction(const char *transactionName);

__attribute__((visibility("default")))
void FalconDaemon2PCFailureCleanupProcessMain(Datum main_arg);

// functions declared for worker transactions
extern void Write2PCRecord(int32 serverId, char *gid);

#endif
