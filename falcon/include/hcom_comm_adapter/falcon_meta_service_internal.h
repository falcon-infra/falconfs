/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_META_SERVICE_INTERNAL_H
#define FALCON_META_SERVICE_INTERNAL_H

#include <vector>

#include "hcom_comm_adapter/hcom_meta_service.h"

extern "C" {
#include "remote_connection_utils/error_code_def.h"
}

namespace falcon {
namespace meta_service {

constexpr size_t FALCON_MAX_NAME_LENGTH = 255;

/**
 * Falcon 元数据服务序列化工具类
 *
 * 提供 FlatBuffers 格式的序列化/反序列化功能
 * 使用 SerializedData 封装 FlatBuffers 数据
 */
class FalconMetaServiceSerializer {
public:
    /**
     * 将 Falcon 元数据请求序列化为 FlatBuffers 格式
     *
     * @param request: Falcon 元数据服务请求
     * @param buffer: SerializedData 封装的 FlatBuffers 数据输出
     * @return: SUCCESS 表示成功，其他错误码表示失败原因
     *
     * 请求格式规范 (SerializedData):
     * [size: 4字节] + [FlatBuffers数据: 对齐后的字节]
     */
    static FalconErrorCode SerializeRequestToSerializedData(
        const FalconMetaServiceRequest& request,
        std::vector<char>& buffer);

    /**
     * 从 FlatBuffers 格式反序列化 Falcon 元数据响应
     *
     * @param data: SerializedData 封装的 FlatBuffers 响应数据
     * @param size: data size
     * @param response: Falcon 元数据服务响应（输出）
     * @param operation: 操作类型
     * @return: true 表示成功，false 表示失败
     *
     * FlatBuffers 响应格式 (SerializedData):
     * [size: 4 bytes] + [FlatBuffers MetaResponse: aligned bytes]
     */
    static bool DeserializeResponseFromSerializedData(
        const void* data,
        size_t size,
        FalconMetaServiceResponse* response,
        FalconMetaOperationType operation);
};

} // namespace meta_service
} // namespace falcon

#endif // FALCON_META_SERVICE_INTERNAL_H
