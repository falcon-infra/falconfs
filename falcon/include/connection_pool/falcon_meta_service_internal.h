/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_META_SERVICE_INTERNAL_H
#define FALCON_META_SERVICE_INTERNAL_H

#include "connection_pool/falcon_meta_service.h"
#include <brpc/controller.h>
#include "falcon_meta_rpc.pb.h"

namespace falcon {
namespace meta_service {

/**
 * Falcon 元数据服务序列化工具类
 *
 * 提供 BINARY 格式的序列化/反序列化功能
 * 依赖 BRPC 和 Protobuf
 */
class FalconMetaServiceSerializer {
public:
    /**
     * 将 Falcon 元数据请求序列化为 BINARY 格式
     *
     * @param request: Falcon 元数据服务请求
     * @param proto_request: Protobuf 请求对象（输出）
     * @param attachment: BRPC 附件（输出）
     * @return: true 表示成功，false 表示失败
     *
     * BINARY 格式规范：
     * [BinaryHeader: 16 bytes]
     * [参数数据: 变长]
     *
     * BinaryHeader 结构：
     * - signature: 0x46414C43 ("FALC")
     * - count: 批处理数量
     * - operation_type: 操作类型
     * - reserved: 保留字段
     */
    static bool SerializeRequestToBinary(
        const FalconMetaServiceRequest& request,
        falcon::meta_proto::MetaRequest* proto_request,
        butil::IOBuf* attachment);

    /**
     * 从 BINARY 格式反序列化 Falcon 元数据响应
     *
     * @param attachment: BRPC 附件（包含二进制响应数据）
     * @param response: Falcon 元数据服务响应（输出）
     * @param operation: 操作类型
     * @return: true 表示成功，false 表示失败
     *
     * BINARY 响应格式（根据操作类型不同而不同）：
     *
     * 1. FETCH_SLICE_ID:
     *    [status: 4 bytes] + [start: 8 bytes] + [end: 8 bytes]
     *
     * 2. SLICE_GET:
     *    [status: 4 bytes] + [slicenum: 4 bytes] + [slice_data: 40*N bytes]
     *
     * 3. SLICE_PUT/DEL:
     *    [status: 4 bytes]
     *
     * 4. 其他操作：参考各自实现
     */
    static bool DeserializeResponseFromBinary(
        const butil::IOBuf& attachment,
        FalconMetaServiceResponse* response,
        FalconMetaOperationType operation);
};

} // namespace meta_service
} // namespace falcon

#endif // FALCON_META_SERVICE_INTERNAL_H
