/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef HCOM_META_SERVICE_H
#define HCOM_META_SERVICE_H

#include <mutex>

#include "hcom_comm_adapter/falcon_meta_service_interface.h"

namespace falcon {
namespace meta_service {

class HcomMetaServiceJob;

class HcomMetaService {
private:
    static HcomMetaService* instance;
    static std::mutex instanceMutex;
    bool initialized;

    HcomMetaService();

public:
    static HcomMetaService* Instance();

    virtual ~HcomMetaService();

    int DispatchHcomMetaServiceJob(HcomMetaServiceJob* job);

    int SubmitFalconMetaRequest(const FalconMetaServiceRequest& request,
                                FalconMetaServiceCallback callback,
                                void* user_context = nullptr);
};

} // namespace meta_service
} // namespace falcon

#endif // HCOM_META_SERVICE_H
