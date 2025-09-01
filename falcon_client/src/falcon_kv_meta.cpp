#include <string>
#include "falcon_kv_meta.h"
#include "cm/falcon_cm.h"
#include "router.h"
#include "utils.h"
#include "log/logging.h"

int FalconPut(FormData_kv_index &kv_index)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByKey(kv_index.key);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }
    int errorCode = conn->Put(kv_index);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->Put(kv_index);
    }
#endif
    // todo:做一层缓存是否
    return errorCode;
}

int FalconGet(FormData_kv_index &kv_index)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByKey(kv_index.key);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }
    int errorCode = conn->Get(kv_index);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->Get(kv_index);
    }
#endif
    // todo:做一层缓存是否
    return errorCode;
}

int FalconDelete(std::string &key)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByKey(key);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }
    int errorCode = conn->Delete(key);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->Delete(key);
    }
#endif
    // todo:做一层缓存是否
    return errorCode;
}