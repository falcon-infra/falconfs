/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "error_code.h"

#include <cerrno>

#include "remote_connection_utils/error_code_def.h"

int ErrorCodeToErrno(int errorCode)
{
    int ret = 0;
    switch (errorCode) {
    case SUCCESS:
        ret = 0;
        break;
    case PATH_IS_INVALID:
        ret = ENOENT;
        break;
    case WRONG_WORKER:
        ret = ESRCH;
        break;
    case OBSOLETE_SHARD:
        ret = ENXIO;
        break;
    case REMOTE_QUERY_FAILED:
        ret = EREMOTEIO;
        break;
    case ARGUMENT_ERROR:
        ret = EINVAL;
        break;
    case INODE_ROW_TYPE_ERROR:
        ret = EILSEQ;
        break;
    case FILE_EXISTS:
        ret = EEXIST;
        break;
    case FILE_NOT_EXISTS:
        ret = ENOENT;
        break;
    case OUT_OF_MEMORY:
        ret = ENOMEM;
        break;
    case STREAM_ERROR:
        ret = EIO;
        break;
    case PROGRAM_ERROR:
        ret = EFAULT;
        break;
    case SMART_ROUTE_ERROR:
        ret = EHOSTUNREACH;
        break;
    case SERVER_FAULT:
        ret = ECONNRESET;
        break;
    case UNDEFINED:
        ret = EAGAIN;
        break;
    case MKDIR_DUPLICATE:
        ret = MKDIR_DUPLICATE;
        break;
    case XKEY_EXISTS:
        ret = EEXIST;
        break;
    case XKEY_NOT_EXISTS:
        ret = ENOENT;
        break;
    // case LINK_EXISTS:
    //     ret = EISCONN;
    //     break;
    // case LINK_NOT_EXISTS:
    //     ret = ENOTCONN;
    //     break;
    case ZK_NOT_CONNECT:
        ret = ENOTCONN;
        break;
    case ZK_FETCH_RESULT_FAILED:
        ret = EIO;
        break;
    case TRANSACTION_FAULT:
        ret = EIO;
        break;
    case POOLED_FAULT:
        ret = EAGAIN;
        break;
    case NOT_FOUND_FD:
        ret = EBADF;
        break;
    case GET_ALL_WORKER_CONN_FAILED:
        ret = EHOSTUNREACH;
        break;
    case IO_ERROR:
        ret = EIO;
        break;
    case PATH_NOT_EXISTS:
        ret = ENOENT;
        break;
    case PATH_VERIFY_FAILED:
        ret = EINVAL;
        break;
    default:
        ret = EIO;
        break;
    }
    return ret;
}
