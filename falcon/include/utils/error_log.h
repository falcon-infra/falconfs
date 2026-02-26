/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_ERROR_LOG_H
#define FALCON_ERROR_LOG_H

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "utils/elog.h"
#include "utils/error_code.h"

#ifdef __cplusplus
}
#endif

#define FALCON_ELOG_INTERNAL(logLevel, logFormat, ...) \
    elog(logLevel, logFormat, ##__VA_ARGS__)

#define FALCON_ELOG_ERROR(errorCode, errorMsg)                                              \
    FALCON_ELOG_INTERNAL(ERROR, "%c %s | %s:%d", ((char)(errorCode) + 64), errorMsg, __FILE__, __LINE__)
#define FALCON_ELOG_ERROR_EXTENDED(errorCode, errorFormat, ...)                             \
    FALCON_ELOG_INTERNAL(ERROR, "%c " errorFormat " | %s:%d",                               \
                         ((char)(errorCode) + 64),                                          \
                         ##__VA_ARGS__,                                                     \
                         __FILE__,                                                          \
                         __LINE__)

#define FALCON_ELOG_WARNING(errorCode, errorMsg)                                            \
    FALCON_ELOG_INTERNAL(WARNING, "%c %s | %s:%d", ((char)(errorCode) + 64), errorMsg, __FILE__, __LINE__)
#define FALCON_ELOG_WARNING_EXTENDED(errorCode, errorFormat, ...)                           \
    FALCON_ELOG_INTERNAL(WARNING, "%c " errorFormat " | %s:%d",                             \
                         ((char)(errorCode) + 64),                                          \
                         ##__VA_ARGS__,                                                     \
                         __FILE__,                                                          \
                         __LINE__)

#define FALCON_ELOG_LOG(logMsg)                                                             \
    FALCON_ELOG_INTERNAL(LOG, "%s | %s:%d", logMsg, __FILE__, __LINE__)
#define FALCON_ELOG_LOG_EXTENDED(logFormat, ...)                                            \
    FALCON_ELOG_INTERNAL(LOG, logFormat " | %s:%d", ##__VA_ARGS__, __FILE__, __LINE__)

#define FALCON_ELOG_FATAL(errorCode, errorMsg)                                              \
    FALCON_ELOG_INTERNAL(FATAL, "%c %s | %s:%d", ((char)(errorCode) + 64), errorMsg, __FILE__, __LINE__)
#define FALCON_ELOG_FATAL_EXTENDED(errorCode, errorFormat, ...)                             \
    FALCON_ELOG_INTERNAL(FATAL, "%c " errorFormat " | %s:%d",                               \
                         ((char)(errorCode) + 64),                                          \
                         ##__VA_ARGS__,                                                     \
                         __FILE__,                                                          \
                         __LINE__)

#define FALCON_ELOG_THREAD_SAFE(logMsg)                                       \
    do {                                                                      \
        fprintf(stderr, "[MT-LOG][%s:%d] %s\n", __FILE__, __LINE__, logMsg);   \
        fflush(stderr);                                                       \
    } while (0)
#define FALCON_ELOG_THREAD_SAFE_EXTENDED(logFormat, ...)                                        \
    do {                                                                                        \
        fprintf(stderr,                                                                          \
                "[MT-LOG][%s:%d] " logFormat "\n",                                               \
                __FILE__,                                                                        \
                __LINE__,                                                                        \
                ##__VA_ARGS__);                                                                  \
        fflush(stderr);                                                                         \
    } while (0)

#endif
