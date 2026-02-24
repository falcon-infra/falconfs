/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_META_SERVICE_H
#define FALCON_META_SERVICE_H

#include <mutex>

#include "hcom_comm_adapter/falcon_meta_service_interface.h"

namespace falcon {
namespace meta_service {

class FalconMetaServiceJob;

class FalconMetaService {
private:
    static FalconMetaService* instance;
    static std::mutex instanceMutex;
    bool initialized;

    FalconMetaService();

public:
    static FalconMetaService* Instance();

    virtual ~FalconMetaService();

    int DispatchFalconMetaServiceJob(FalconMetaServiceJob* job);

    int SubmitFalconMetaRequest(const FalconMetaServiceRequest& request,
                                FalconMetaServiceCallback callback,
                                void* user_context = nullptr);
};

} // namespace meta_service
} // namespace falcon

#endif // FALCON_META_SERVICE_H
