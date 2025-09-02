#ifndef FALCONFS_KV_IPC_MESSGAE_H
#define FALCONFS_KV_IPC_MESSGAE_H

#include <cstdint>
#include <functional>
#include <iomanip>
#include <vector>
#include <string>
#include "hcom/kv_hcom_service.h"
#include "log/logging.h"

constexpr uint32_t EP_SIZE = 4;
constexpr uint32_t SEND_SIZE = 16;
constexpr uint32_t RECEIVE_SIZE_PER_QP = 32;
constexpr uint32_t QUEUE_SIZE = 64;
constexpr uint32_t SEG_COUNT = 1024;
constexpr uint32_t RECEIVE_SEG_SIZE = 1024;
constexpr uint32_t MAX_MESSAGE_SIZE = 1048576;
constexpr uint16_t CHANNEL_DEFAULT_TIMEOUT = 60;
constexpr uint64_t MAX_SHARED_FILE_SIZE = 1099511627776L; /* 1TB */

// TODO 后续配置文件读取 可设置
constexpr uint64_t DEFAULT_SHARED_FILE_SIZE = 1024 * 1024 * 1024; /* mmap size 1G */
constexpr uint32_t KEY_MAX_LEN = 2048;
constexpr uint32_t PRINTABLE_WIDTH = 2;

enum KvOpCode {
    IPC_OP_KV_ALLOCATE_MORE_BLOCK = 0,
    IPC_OP_KV_GET_SHARED_FILE_INFO = 1,
    IPC_OP_KV_PUT_SHM_FINISH = 2,
    IPC_OP_KV_GET_FROM_SHM = 3,
    IPC_OP_KV_DELETE = 4,

};


using NewRequestHandler = std::function<int(Service_Context ctx, uint64_t usrCtx)>;
using NewChannelHandler = std::function<int(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad)>;
using ChannelBrokenHandler = std::function<void(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad)>;


#define DECLARE_CHAR_ARRAY_SET_FUNC(func, CHAR_ARRAY)    \
    bool func(const std::string &other)                  \
    {                                                    \
        if (other.length() > (sizeof(CHAR_ARRAY) - 1)) { \
            return false;                                \
        }                                                \
                                                         \
        for (uint32_t i = 0; i < other.length(); i++) {  \
            CHAR_ARRAY[i] = other.at(i);                 \
        }                                                \
                                                         \
        (CHAR_ARRAY)[other.length()] = '\0';             \
        return true;                                     \
    }

#define DECLARE_CHAR_ARRAY_GET_FUNC(func, CHAR_ARRAY)                   \
    std::string func() const                                            \
    {                                                                   \
        return { CHAR_ARRAY, strnlen(CHAR_ARRAY, sizeof(CHAR_ARRAY)) }; \
    }

static inline std::string PrintableString(const std::string &str) noexcept
{
    std::ostringstream oss;
    for (auto &ch : str) {
        if (isprint(ch)) {
            oss << ch;
        } else {
            oss << std::hex << "\\0x" << std::setw(PRINTABLE_WIDTH) << std::setfill('0') <<
                static_cast<uint32_t>(static_cast<uint8_t>(ch)) << std::oct;
        }
    }
    return oss.str();
}

struct KvSharedFileInfoReq {
    int32_t flags = 0;
};

struct KvSharedFileInfoResp {
    int32_t result = 0;
    uint64_t shardFileSize;

    std::string ToString() const
    {
        std::ostringstream oss;
        oss << "result " << result << " shardFileSize: " << shardFileSize;
        return oss.str();
    }
};

struct KvOperationReq {
    int32_t flags = 0; /* flags */
    uint32_t valueLen = 0; /* value size */
    char key[KEY_MAX_LEN]{}; /* key */

    DECLARE_CHAR_ARRAY_SET_FUNC(Key, key);
    DECLARE_CHAR_ARRAY_GET_FUNC(Key, key);

    std::string ToString() const
    {
        std::ostringstream oss;
        std::string key = PrintableString(Key());
        oss << "flags: " << flags << " valueLen: " << valueLen << " key: " << key;
        return oss.str();
    }
};

struct KvOperationResp {
    int32_t result = 0;
    int32_t flags = 0;
    uint32_t valueLen = 0;

    std::string ToString() const
    {
        std::ostringstream oss;
        oss << "flags: " << flags << " result: " << result << " valueLen: " << valueLen;
        return oss.str();
    }
};

#endif // FALCONFS_KV_IPC_MESSGAE_H
