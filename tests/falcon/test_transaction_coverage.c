#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "postgres.h"
#include "access/xact.h"
#include "utils/guc.h"

#ifdef vsnprintf
#undef vsnprintf
#endif
#ifdef snprintf
#undef snprintf
#endif

static int g_begin_count;
static int g_commit_command_count;
static int g_abort_block_count;
static int g_prepare_block_count;
static int g_finish_prepared_commit_count;
static int g_finish_prepared_abort_count;
static int g_prevent_count;
static int g_set_config_result;
static bool g_end_transaction_result;
static bool g_local_write;
static int g_local_server_id;
static TransactionId g_top_transaction_id;
static XactCallback g_xact_callback;
static SubXactCallback g_subxact_callback;
static int g_remote_prepare_count;
static int g_remote_commit_count;
static int g_remote_abort_count;
static bool g_remote_abort_result;
static int g_clear_remote_command_count;
static int g_remove_inprogress_count;
static int g_path_reset_count;
static int g_dir_commit_count;
static int g_dir_abort_count;
static int g_rw_release_all_count;
static bool g_last_release_all_keep;
static int g_held_count;
static int g_release_since_count;
static int g_last_release_since_saved;
static bool g_last_release_since_keep;

static void ResetTransactionHarness(void)
{
    g_begin_count = 0;
    g_commit_command_count = 0;
    g_abort_block_count = 0;
    g_prepare_block_count = 0;
    g_finish_prepared_commit_count = 0;
    g_finish_prepared_abort_count = 0;
    g_prevent_count = 0;
    g_set_config_result = 1;
    g_end_transaction_result = true;
    g_local_write = true;
    g_local_server_id = 7;
    g_top_transaction_id = 12345;
    g_xact_callback = NULL;
    g_subxact_callback = NULL;
    g_remote_prepare_count = 0;
    g_remote_commit_count = 0;
    g_remote_abort_count = 0;
    g_remote_abort_result = true;
    g_clear_remote_command_count = 0;
    g_remove_inprogress_count = 0;
    g_path_reset_count = 0;
    g_dir_commit_count = 0;
    g_dir_abort_count = 0;
    g_rw_release_all_count = 0;
    g_last_release_all_keep = false;
    g_held_count = 0;
    g_release_since_count = 0;
    g_last_release_since_saved = -1;
    g_last_release_since_keep = false;
}

static int ExpectTrue(bool condition, const char *message)
{
    if (!condition) {
        (void)message;
        return 1;
    }
    return 0;
}

void RemoveInprogressTransaction(const char *gid)
{
    (void)gid;
    g_remove_inprogress_count++;
}

StringInfo makeStringInfo(void)
{
    StringInfo str = calloc(1, sizeof(StringInfoData));
    str->maxlen = 256;
    str->data = calloc(1, str->maxlen);
    return str;
}

void appendStringInfo(StringInfo str, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int len = vsnprintf(str->data + str->len, str->maxlen - str->len, fmt, args);
    va_end(args);
    if (len > 0)
        str->len += len;
}

TransactionId GetTopTransactionId(void)
{
    return g_top_transaction_id;
}

int32_t GetLocalServerId(void)
{
    return g_local_server_id;
}

bool IsLocalWrite(void)
{
    return g_local_write;
}

void BeginTransactionBlock(void)
{
    g_begin_count++;
}

bool EndTransactionBlock(bool chain)
{
    (void)chain;
    return g_end_transaction_result;
}

void UserAbortTransactionBlock(bool chain)
{
    (void)chain;
    g_abort_block_count++;
}

bool PrepareTransactionBlock(const char *gid)
{
    (void)gid;
    g_prepare_block_count++;
    return true;
}

void PreventInTransactionBlock(bool is_top_level, const char *stmt_type)
{
    (void)is_top_level;
    (void)stmt_type;
    g_prevent_count++;
}

void FinishPreparedTransaction(const char *gid, bool is_commit)
{
    (void)gid;
    if (is_commit)
        g_finish_prepared_commit_count++;
    else
        g_finish_prepared_abort_count++;
}

void CommitTransactionCommand(void)
{
    g_commit_command_count++;
}

int set_config_option(const char *name, const char *value, GucContext context, GucSource source,
                      GucAction action, bool change_val, int elevel, bool is_reload)
{
    (void)name;
    (void)value;
    (void)context;
    (void)source;
    (void)action;
    (void)change_val;
    (void)elevel;
    (void)is_reload;
    return g_set_config_result;
}

bool superuser(void)
{
    return false;
}

void RegisterXactCallback(XactCallback callback, void *arg)
{
    (void)arg;
    g_xact_callback = callback;
}

void RegisterSubXactCallback(SubXactCallback callback, void *arg)
{
    (void)arg;
    g_subxact_callback = callback;
}

void FalconRemoteCommandPrepare(void)
{
    g_remote_prepare_count++;
}

void FalconRemoteCommandCommit(void)
{
    g_remote_commit_count++;
}

bool FalconRemoteCommandAbort(void)
{
    g_remote_abort_count++;
    return g_remote_abort_result;
}

void ClearRemoteConnectionCommand(void)
{
    g_clear_remote_command_count++;
}

void TransactionLevelPathParseReset(void)
{
    g_path_reset_count++;
}

void CommitForDirPathHash(void)
{
    g_dir_commit_count++;
}

void AbortForDirPathHash(void)
{
    g_dir_abort_count++;
}

void RWLockReleaseAll(bool keep_interrupt_holdoff_count)
{
    g_rw_release_all_count++;
    g_last_release_all_keep = keep_interrupt_holdoff_count;
}

int RWLockGetHeldCount(void)
{
    return g_held_count;
}

void RWLockReleaseSince(int saved_count, bool keep_interrupt_holdoff_count)
{
    g_release_since_count++;
    g_last_release_since_saved = saved_count;
    g_last_release_since_keep = keep_interrupt_holdoff_count;
}

void FalconEnterAbortProgress(void) {}

void FalconQuitAbortProgress(void) {}

bool errstart(int elevel, const char *domain)
{
    (void)elevel;
    (void)domain;
    return true;
}

bool errstart_cold(int elevel, const char *domain)
{
    return errstart(elevel, domain);
}

void errfinish(const char *filename, int lineno, const char *funcname)
{
    (void)filename;
    (void)lineno;
    (void)funcname;
}

int errcode(int sqlerrcode)
{
    return sqlerrcode;
}

int errmsg(const char *fmt, ...)
{
    (void)fmt;
    return 0;
}

int errmsg_internal(const char *fmt, ...)
{
    (void)fmt;
    return 0;
}

void elog_start(const char *filename, int lineno, const char *funcname)
{
    (void)filename;
    (void)lineno;
    (void)funcname;
}

void elog_finish(int elevel, const char *fmt, ...)
{
    (void)elevel;
    (void)fmt;
}

int pg_vsnprintf(char *str, size_t count, const char *fmt, va_list args)
{
    return vsnprintf(str, count, fmt, args);
}

int pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int ret = vsnprintf(str, count, fmt, args);
    va_end(args);
    return ret;
}

#include "../../falcon/transaction/transaction.c"

static int TestExplicitLifecycle(void)
{
    ResetTransactionHarness();
    g_top_transaction_id = 55;
    g_local_server_id = 3;
    g_local_write = false;
    StringInfo gid = GetImplicitTransactionGid();
    if (ExpectTrue(strstr(gid->data, "55:3:F") != NULL, "implicit gid records xid/server/write flag"))
        return 1;

    FalconExplicitTransactionBegin();
    if (ExpectTrue(g_begin_count == 1 && g_commit_command_count == 1, "begin starts and commits command"))
        return 1;
    if (ExpectTrue(FalconExplicitTransactionCommit(), "commit returns EndTransactionBlock result"))
        return 1;

    FalconExplicitTransactionBegin();
    FalconExplicitTransactionRollback();
    if (ExpectTrue(g_abort_block_count == 1, "rollback calls UserAbortTransactionBlock"))
        return 1;

    FalconExplicitTransactionBegin();
    if (ExpectTrue(FalconExplicitTransactionPrepare("gid-prepare"), "prepare returns true"))
        return 1;

    falconExplicitTransactionState = FALCON_EXPLICIT_TRANSACTION_PREPARED;
    FalconExplicitTransactionCommitPrepared("gid-commit");
    falconExplicitTransactionState = FALCON_EXPLICIT_TRANSACTION_PREPARED;
    FalconExplicitTransactionRollbackPrepared("gid-rollback");
    return ExpectTrue(g_prevent_count == 2 && g_finish_prepared_commit_count == 1 &&
                          g_finish_prepared_abort_count == 1,
                      "prepared commit and rollback both finish");
}

static int TestTransactionCallbacks(void)
{
    ResetTransactionHarness();
    RegisterFalconTransactionCallback();
    if (ExpectTrue(g_xact_callback != NULL && g_subxact_callback != NULL, "callbacks registered"))
        return 1;

    g_xact_callback(XACT_EVENT_PRE_COMMIT, NULL);
    if (ExpectTrue(g_remote_prepare_count == 1, "pre-commit prepares remote commands"))
        return 1;

    strcpy(RemoteTransactionGid, "remote-gid");
    g_xact_callback(XACT_EVENT_COMMIT, NULL);
    if (ExpectTrue(g_remote_commit_count == 1 && g_path_reset_count == 1 && g_dir_commit_count == 1,
                   "commit callback clears local state"))
        return 1;
    if (ExpectTrue(g_rw_release_all_count == 1 && !g_last_release_all_keep &&
                       g_remove_inprogress_count == 1 && RemoteTransactionGid[0] == '\0',
                   "commit callback releases locks and remote gid"))
        return 1;

    FalconExplicitTransactionBegin();
    strcpy(RemoteTransactionGid, "abort-gid");
    g_remote_abort_result = false;
    g_xact_callback(XACT_EVENT_ABORT, NULL);
    if (ExpectTrue(g_dir_abort_count == 1 && g_remote_abort_count == 1 && g_last_release_all_keep,
                   "abort callback releases abort state"))
        return 1;
    if (ExpectTrue(g_remove_inprogress_count == 2 && g_abort_block_count == 1,
                   "abort callback clears gid and explicit state"))
        return 1;

    g_xact_callback(XACT_EVENT_PREPARE, NULL);
    g_xact_callback(XACT_EVENT_PRE_PREPARE, NULL);
    g_xact_callback(XACT_EVENT_PARALLEL_COMMIT, NULL);
    g_xact_callback(XACT_EVENT_PARALLEL_PRE_COMMIT, NULL);
    g_xact_callback(XACT_EVENT_PARALLEL_ABORT, NULL);
    return ExpectTrue(g_path_reset_count == 3, "prepare callback resets path state");
}

static int TestSubTransactionCallbacks(void)
{
    ResetTransactionHarness();
    RegisterFalconTransactionCallback();
    if (ExpectTrue(g_subxact_callback != NULL, "subtransaction callback registered"))
        return 1;

    g_held_count = 2;
    g_subxact_callback(SUBXACT_EVENT_START_SUB, 2, 1, NULL);
    g_held_count = 5;
    g_subxact_callback(SUBXACT_EVENT_ABORT_SUB, 2, 1, NULL);
    if (ExpectTrue(g_release_since_count == 1 && g_last_release_since_saved == 2 &&
                       g_last_release_since_keep,
                   "abort subtransaction releases locks acquired after saved count"))
        return 1;

    g_held_count = 4;
    g_subxact_callback(SUBXACT_EVENT_START_SUB, 3, 1, NULL);
    g_subxact_callback(SUBXACT_EVENT_COMMIT_SUB, 3, 1, NULL);
    if (ExpectTrue(g_release_since_count == 1, "commit subtransaction only pops stack"))
        return 1;

    g_subxact_callback(SUBXACT_EVENT_PRE_COMMIT_SUB, 4, 1, NULL);
    return 0;
}

int main(void)
{
    if (TestExplicitLifecycle())
        return 1;
    if (TestTransactionCallbacks())
        return 1;
    if (TestSubTransactionCallbacks())
        return 1;
    return 0;
}
