/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/falcon_connection_pool.h"

#include <dlfcn.h>
#include <elf.h>
#include <fcntl.h>
#include <link.h>
#include <signal.h>
#include <stdlib.h>
#include <threads.h>
#include <unistd.h>

#include "postgres.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/shmem.h"
#include "utils/error_log.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/utils.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "base_comm_adapter/comm_server_interface.h"
#include "brpc_comm_adapter/falcon_brpc_server.h"
#include "connection_pool/pg_connection_pool.h"
#include "control/control_flag.h"

int FalconPGPort = 0;
int FalconConnectionPoolPort = FALCON_CONNECTION_POOL_PORT_DEFAULT;
int FalconConnectionPoolSize = FALCON_CONNECTION_POOL_SIZE_DEFAULT;
int FalconConnectionPoolBatchSize = FALCON_CONNECTION_POOL_BATCH_SIZE_DEFAULT;
int FalconConnectionPoolWaitAdjust = FALCON_CONNECTION_POOL_WAIT_ADJUST_DEFAULT;
int FalconConnectionPoolWaitMin = FALCON_CONNECTION_POOL_WAIT_MIN_DEFAULT;
int FalconConnectionPoolWaitMax = FALCON_CONNECTION_POOL_WAIT_MAX_DEFAULT;
uint64_t FalconConnectionPoolShmemSize = FALCON_CONNECTION_POOL_SHMEM_SIZE_DEFAULT;

// communication plugin path, using global variable for shared to worker process
char *FalconCommunicationPluginPath;
// communication server IP, using global variable for shared to worker process
char *FalconCommunicationServerIp;

char *FalconNodeLocalIp = NULL;

// variable used for falcon communication plugin
static falcon_plugin_start_comm_func_t comm_work_func = NULL;
static falcon_plugin_stop_comm_func_t comm_cleanup_func = NULL;
static falcon_plugin_flush_coverage_func_t comm_flush_coverage_func = NULL;
static void (*comm_plugin_gcov_dump_func)(void) = NULL;
static void *falcon_comm_dl_handle = NULL;

static volatile bool got_SIGTERM = false;
static void FalconDaemonConnectionPoolProcessSigTermHandler(SIGNAL_ARGS);

static void *ResolveLocalSymbolAddress(void *handle, const char *symbolName)
{
    struct link_map *linkMap = NULL;
    Elf64_Ehdr elfHeader;
    Elf64_Shdr *sectionHeaders = NULL;
    char *sectionNames = NULL;
    void *resolvedAddress = NULL;
    int fd = -1;

    if (handle == NULL || symbolName == NULL) {
        return NULL;
    }
    if (dlinfo(handle, RTLD_DI_LINKMAP, &linkMap) != 0 || linkMap == NULL || linkMap->l_name == NULL ||
        linkMap->l_name[0] == '\0') {
        return NULL;
    }

    fd = open(linkMap->l_name, O_RDONLY);
    if (fd < 0) {
        return NULL;
    }
    if (read(fd, &elfHeader, sizeof(elfHeader)) != sizeof(elfHeader)) {
        goto cleanup;
    }
    if (memcmp(elfHeader.e_ident, ELFMAG, SELFMAG) != 0 || elfHeader.e_ident[EI_CLASS] != ELFCLASS64 ||
        elfHeader.e_shentsize != sizeof(Elf64_Shdr) || elfHeader.e_shnum == 0) {
        goto cleanup;
    }

    sectionHeaders = (Elf64_Shdr *)malloc(elfHeader.e_shentsize * elfHeader.e_shnum);
    if (sectionHeaders == NULL) {
        goto cleanup;
    }
    if (lseek(fd, elfHeader.e_shoff, SEEK_SET) < 0 ||
        read(fd, sectionHeaders, elfHeader.e_shentsize * elfHeader.e_shnum) !=
            elfHeader.e_shentsize * elfHeader.e_shnum) {
        goto cleanup;
    }
    if (elfHeader.e_shstrndx >= elfHeader.e_shnum) {
        goto cleanup;
    }

    sectionNames = (char *)malloc(sectionHeaders[elfHeader.e_shstrndx].sh_size);
    if (sectionNames == NULL) {
        goto cleanup;
    }
    if (lseek(fd, sectionHeaders[elfHeader.e_shstrndx].sh_offset, SEEK_SET) < 0 ||
        read(fd, sectionNames, sectionHeaders[elfHeader.e_shstrndx].sh_size) !=
            (ssize_t)sectionHeaders[elfHeader.e_shstrndx].sh_size) {
        goto cleanup;
    }

    for (int i = 0; i < elfHeader.e_shnum; i++) {
        Elf64_Shdr symbolSection = sectionHeaders[i];
        Elf64_Shdr stringSection;
        Elf64_Sym *symbols = NULL;
        char *symbolNames = NULL;
        size_t symbolCount = 0;

        if (symbolSection.sh_type != SHT_SYMTAB || symbolSection.sh_link >= elfHeader.e_shnum ||
            strcmp(sectionNames + symbolSection.sh_name, ".symtab") != 0) {
            continue;
        }

        stringSection = sectionHeaders[symbolSection.sh_link];
        symbolCount = symbolSection.sh_size / symbolSection.sh_entsize;
        symbols = (Elf64_Sym *)malloc(symbolSection.sh_size);
        symbolNames = (char *)malloc(stringSection.sh_size);
        if (symbols == NULL || symbolNames == NULL) {
            free(symbols);
            free(symbolNames);
            goto cleanup;
        }

        if (lseek(fd, symbolSection.sh_offset, SEEK_SET) < 0 ||
            read(fd, symbols, symbolSection.sh_size) != (ssize_t)symbolSection.sh_size ||
            lseek(fd, stringSection.sh_offset, SEEK_SET) < 0 ||
            read(fd, symbolNames, stringSection.sh_size) != (ssize_t)stringSection.sh_size) {
            free(symbols);
            free(symbolNames);
            goto cleanup;
        }

        for (size_t symbolIdx = 0; symbolIdx < symbolCount; symbolIdx++) {
            if (symbols[symbolIdx].st_name >= stringSection.sh_size ||
                strcmp(symbolNames + symbols[symbolIdx].st_name, symbolName) != 0 ||
                ELF64_ST_TYPE(symbols[symbolIdx].st_info) != STT_FUNC || symbols[symbolIdx].st_value == 0) {
                continue;
            }
            resolvedAddress = (void *)(linkMap->l_addr + symbols[symbolIdx].st_value);
            free(symbols);
            free(symbolNames);
            goto cleanup;
        }

        free(symbols);
        free(symbolNames);
    }

cleanup:
    free(sectionHeaders);
    free(sectionNames);
    if (fd >= 0) {
        close(fd);
    }
    return resolvedAddress;
}

static inline void FlushCoverageData(void)
{
    void (*gcov_dump)(void) = (void (*)(void))dlsym(RTLD_DEFAULT, "__gcov_dump");
    if (gcov_dump != NULL) {
        gcov_dump();
    }
}

static inline void FlushCoverageDataForPlugin(void)
{
    if (comm_flush_coverage_func != NULL) {
        comm_flush_coverage_func();
    }
    if (comm_plugin_gcov_dump_func != NULL) {
        comm_plugin_gcov_dump_func();
    }
}

void FalconDaemonConnectionPoolProcessMain(unsigned long int main_arg)
{
    pqsignal(SIGTERM, FalconDaemonConnectionPoolProcessSigTermHandler);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    CurrentResourceOwner = ResourceOwnerCreate(NULL, "falcon connection pool");
    CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext,
                                                 "falcon connection pool context",
                                                 ALLOCSET_DEFAULT_MINSIZE,
                                                 ALLOCSET_DEFAULT_INITSIZE,
                                                 ALLOCSET_DEFAULT_MAXSIZE);
    elog(LOG, "FalconDaemonConnectionPoolProcessMain: pid = %d", getpid());
    elog(LOG, "FalconDaemonConnectionPoolProcessMain: wait init.");
    bool falconHasBeenLoad = false;
    while (true) {
        StartTransactionCommand();
        falconHasBeenLoad = CheckFalconHasBeenLoaded();
        CommitTransactionCommand();
        if (falconHasBeenLoad) {
            break;
        }
        sleep(1);
    }
    do {
        sleep(1);
    } while (RecoveryInProgress());
    elog(LOG, "FalconDaemonConnectionPoolProcessMain: init finished.");

    // PostPortNumber need using both here and falcon_run_pooler_server_func, so set to global variable FalconPGPort
    FalconPGPort = PostPortNumber;
    RunConnectionPoolServer();
    elog(LOG, "FalconDaemonConnectionPoolProcessMain: connection pool server stopped.");
    return;
}

static void FalconDaemonConnectionPoolProcessSigTermHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    FlushCoverageData();
    FlushCoverageDataForPlugin();

    elog(LOG, "FalconDaemonConnectionPoolProcessSigTermHandler: get sigterm.");
    got_SIGTERM = true;

    DestroyPGConnectionPool();
    if (comm_cleanup_func != NULL) {
        comm_cleanup_func();
        comm_cleanup_func = NULL;
    }

    if (falcon_comm_dl_handle != NULL) {
        FlushCoverageDataForPlugin();
        dlclose(falcon_comm_dl_handle);
        comm_work_func = NULL;
        comm_flush_coverage_func = NULL;
        comm_plugin_gcov_dump_func = NULL;
        falcon_comm_dl_handle = NULL;
    }

    FlushCoverageData();

    errno = save_errno;
}

size_t FalconConnectionPoolShmemsize(void) { return FalconConnectionPoolShmemSize; }

void FalconConnectionPoolShmemInit(void)
{
    bool initialized;
    char *falconConnectionPoolShmemBuffer =
        ShmemInitStruct("Falcon Connection Pool Shmem", FalconConnectionPoolShmemsize(), &initialized);

    FalconShmemAllocator *allocator = GetFalconConnectionPoolShmemAllocator();
    if (FalconShmemAllocatorInit(allocator, falconConnectionPoolShmemBuffer, FalconConnectionPoolShmemsize()) != 0) {
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "FalconShmemAllocatorInit failed.");
    }

    if (!initialized) {
        memset(allocator->signatureCounter,
               0,
               sizeof(PaddedAtomic64) * (1 + FALCON_SHMEM_ALLOCATOR_FREE_LIST_COUNT + allocator->pageCount));
    }
}

static void StartCommunicationSever()
{
    /* Load communication plugin */
    if (FalconCommunicationPluginPath == NULL) {
        elog(
            ERROR,
            "Communication plugin not provide, please check the falcon_communication.plugin_path in postgresql.conf. ");
        return;
    }
    elog(LOG, "Using plugin %s start CommunicationSever.", FalconCommunicationPluginPath);
// MARK:Use RTLD_GLOBAL so symbols from the comm plugin are visible
// to secodary plugins loaded later by the comm plugin
    falcon_comm_dl_handle = dlopen(FalconCommunicationPluginPath, RTLD_LAZY | RTLD_GLOBAL);
    if (!falcon_comm_dl_handle) {
        elog(ERROR,
             "Failed to load plugin in background worker: %s, error: %s",
             FalconCommunicationPluginPath,
             dlerror());
        return;
    }

    comm_work_func = (falcon_plugin_start_comm_func_t)dlsym(falcon_comm_dl_handle, FALCON_PLUGIN_START_COMM_FUNC_NAME);
    comm_cleanup_func = (falcon_plugin_stop_comm_func_t)dlsym(falcon_comm_dl_handle, FALCON_PLUGIN_STOP_COMM_FUNC_NAME);
    comm_flush_coverage_func =
        (falcon_plugin_flush_coverage_func_t)dlsym(falcon_comm_dl_handle, FALCON_PLUGIN_FLUSH_COVERAGE_FUNC_NAME);
    comm_plugin_gcov_dump_func = (void (*)(void))ResolveLocalSymbolAddress(falcon_comm_dl_handle, "__gcov_dump");
    if (!comm_work_func || !comm_cleanup_func) {
        elog(ERROR, "Plugin %s missing required functions (work/cleanup)", FalconCommunicationPluginPath);
        dlclose(falcon_comm_dl_handle);
        return;
    }

    /* Execute plugin work */
    int ret =
        comm_work_func(FalconDispatchMetaJob2PGConnectionPool, FalconCommunicationServerIp, FalconConnectionPoolPort);
    if (ret != 0) {
        elog(ERROR, "Plugin work function returned %d: %s", ret, FalconCommunicationPluginPath);
        dlclose(falcon_comm_dl_handle);
        return;
    }
    /* Cleanup */
    elog(LOG, "Background worker stopping: %s", FalconCommunicationPluginPath);
    FlushCoverageData();
    FlushCoverageDataForPlugin();
    comm_cleanup_func();
    comm_cleanup_func = NULL;

    FlushCoverageDataForPlugin();
    dlclose(falcon_comm_dl_handle);
    comm_work_func = NULL;
    comm_flush_coverage_func = NULL;
    comm_plugin_gcov_dump_func = NULL;
    falcon_comm_dl_handle = NULL;
    FlushCoverageData();
}

void RunConnectionPoolServer(void)
{
    // start PGConnectionPool wait for dispatch jobs
    bool ret = StartPGConnectionPool();
    if (!ret) {
        elog(ERROR, "RunConnectionPoolServer: StartPGConnectionPool failed.");
    }

    // start Communication Server receive jobs and dispatch jobs to PGConnectionPool
    StartCommunicationSever();
}
