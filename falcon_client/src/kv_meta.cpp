#include "log/logging.h"
#include "kv_meta.h"
#include "kv_client_operation.h"
#include "kv_ipc_message.h"
#include "kv_service_operation.h"
#include "kv_utils.h"

static KvClientOperation *g_kvClientOperation = KvClientOperation::Instance();

int FalconKvInit(std::string &path)
{
    std::string hcomLibPath = "/home/wxt/libhcom.so";
    auto ret = InitKvHcomIpcDl(hcomLibPath, hcomLibPath.size());
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Falcon kv ipc init failed";
        return ret;
    }

    if (path.empty()) {
        FALCON_LOG(LOG_ERROR) << "invalid param";
        return -1;
    }
    if (g_kvClientOperation == nullptr) {
        FALCON_LOG(LOG_ERROR) << "kv client operation is nullptr";
        return -1;
    }
    return g_kvClientOperation->Initialize(path);
}

int FalconKvShmAndIpcServiceInit(void)
{
    auto g_kvServiceOperation = KvServiceOperation::Instance();
    return g_kvServiceOperation->KvShmAndIpcServiceInit();
}

int FalconKvPutData(const std::string &key, const void* vaule, const size_t len)
{
    if (key.empty() || vaule == nullptr || len == 0) {
        FALCON_LOG(LOG_ERROR) << "invalid param";
        return -1;
    }
    if (g_kvClientOperation == nullptr) {
        FALCON_LOG(LOG_ERROR) << "kv client operation is nullptr";
        return -1;
    }
    return g_kvClientOperation->KvPutShmData(key, vaule, len);
}

int FalconKvGetData(const std::string &key, void* vaule)
{
    if (key.empty() || vaule == nullptr) {
        FALCON_LOG(LOG_ERROR) << "invalid param";
        return -1;
    }
    if (g_kvClientOperation == nullptr) {
        FALCON_LOG(LOG_ERROR) << "kv client operation is nullptr";
        return -1;
    }

    return g_kvClientOperation->KvGetShmData(key, vaule);
}

int FalconKvDeleteKey(const std::string &key)
{
    if (key.empty()) {
        FALCON_LOG(LOG_ERROR) << "invalid param";
        return -1;
    }
    if (g_kvClientOperation == nullptr) {
        FALCON_LOG(LOG_ERROR) << "kv client operation is nullptr";
        return -1;
    }

    return g_kvClientOperation->KvDeleteKey(key);
}