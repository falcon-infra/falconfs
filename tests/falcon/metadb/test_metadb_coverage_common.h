#pragma once

#include "dfs.h"
#include "falcon_meta_param_generated.h"
#include "local_run_workload_test_common.h"
#include "utils/falcon_meta_service_def.h"

extern "C" {
#include "metadb/meta_process_info.h"
}

#include "metadb/meta_serialize_interface_helper.h"

#include <gtest/gtest.h>
#include <libpq-fe.h>
#include <fmt/format.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

extern int thread_num;
extern int client_cache_size;
extern int files_per_dir;
extern int file_size;
extern int file_num;
extern std::atomic<bool> printed;
extern volatile uint64_t op_count[16384];
extern volatile uint64_t latency_count[16384];

namespace metadb_test {

std::string BuildRootPath(const char *tag);
std::string ThreadDir(const std::string &root, int thread_id);
std::string FilePath(const std::string &root, int thread_id, int file_id);
std::string DirPath(const std::string &root, int thread_id, int dir_id);

class SerializedDataGuard {
  public:
    SerializedDataGuard();
    ~SerializedDataGuard();
    SerializedData *get();

  private:
    SerializedData data_{};
};

void InitNamespaceRoot(const std::string &root);
void UninitNamespaceRoot(const std::string &root);
bool InitClientOrSkip();
void CleanupRoot(const std::string &root, const std::string &file_path, const std::string &dir_path);
std::string SqlQuote(const std::string &value);
std::string HexEncode(const std::vector<uint8_t> &bytes);

std::vector<uint8_t> BuildPathOnlyParam(const std::string &path);
std::vector<uint8_t> BuildCloseParam(const std::string &path, int64_t size, uint64_t mtime, int32_t node_id);
std::vector<uint8_t> BuildReadDirParam(const std::string &path, int32_t max_read_count,
                                       int32_t last_shard_index, const std::string &last_file_name);
std::vector<uint8_t> BuildRenameParam(const std::string &src, const std::string &dst);
std::vector<uint8_t> BuildUtimeNsParam(const std::string &path, uint64_t atime, uint64_t mtime);
std::vector<uint8_t> BuildChownParam(const std::string &path, uint32_t uid, uint32_t gid);
std::vector<uint8_t> BuildChmodParam(const std::string &path, uint64_t mode);
std::vector<uint8_t> BuildKvParam(const std::string &key, uint32_t value_len,
                                  const std::vector<uint64_t> &value_key,
                                  const std::vector<uint64_t> &location,
                                  const std::vector<uint32_t> &size);
std::vector<uint8_t> BuildKeyOnlyParam(const std::string &key);
std::vector<uint8_t> BuildSliceInfoParam(const std::string &name, const std::vector<uint64_t> &inode_id,
                                         const std::vector<uint32_t> &chunk_id,
                                         const std::vector<uint64_t> &slice_id,
                                         const std::vector<uint32_t> &slice_size,
                                         const std::vector<uint32_t> &slice_offset,
                                         const std::vector<uint32_t> &slice_len,
                                         const std::vector<uint32_t> &slice_loc1,
                                         const std::vector<uint32_t> &slice_loc2);
std::vector<uint8_t> BuildSliceIndexParam(const std::string &name, uint64_t inode_id, uint32_t chunk_id);
std::vector<uint8_t> BuildSliceIdParam(uint32_t count, uint8_t type);

class PgConnection {
  public:
    PgConnection(const std::string &host, int port);
    bool IsOpen() const;
    std::string ErrorMessage() const;
    bool ExecOk(const std::string &sql);
    bool ScalarText(const std::string &sql, std::string *value);
    bool ScalarInt(const std::string &sql, int *value);
    bool SerializedCall(FalconMetaServiceType type, const std::vector<uint8_t> &param, int *response_size);

  private:
    std::unique_ptr<PGconn, decltype(&PQfinish)> conn_{nullptr, PQfinish};
};

bool ConnectPlainSql(int pg_port, PgConnection *&connection_holder, std::unique_ptr<PgConnection> &owner);

}  // 命名空间 metadb_test
