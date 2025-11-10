#include <string>
#include "falcon_slice_id.h"
#include "cm/falcon_cm.h"
#include "router.h"
#include "utils.h"
#include "log/logging.h"

extern std::shared_ptr<Router> router;

std::pair<uint64_t, uint64_t> sliceKeyRangeFetch(uint32_t count, SliceIdType type)
{
    std::shared_ptr<Connection> conn = router->GetCoordinatorConn();
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return {0, 0};
    }

    std::pair<uint64_t, uint64_t> sliceIds {};
    int errorCode = conn->FetchSliceId(count, type, sliceIds);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateCNConn(conn);
        errorCode = conn->FetchSliceId(count, type, sliceIds);
    }
#endif

    if (errorCode != SUCCESS) {
        sliceIds = {0, 0};
        FALCON_LOG(LOG_ERROR) << "falcon sliceKeyRangeFetch failed, error code: " << errorCode;
    }

    return sliceIds;
}