// SSD spill manager (v6 §12.4).
//
// Path layout: <ssd_root>/<store_node_id>/<hash_prefix>/<block_hash>.<version>.kv
// `block_hash` is hex-encoded for safe filenames. All paths are validated to
// be canonical, prefixed by `<ssd_root>`, and free of `..` traversal.
//
// Spill writes the payload, then `fdatasync()` to ensure durability before
// returning the path. Reads validate the path and return raw bytes.
#pragma once

#include <cstdint>
#include <string>

namespace falconfs::kv {

struct SSDSpillResult {
    bool ok = false;
    std::string evicted_path;
    std::string error_message;
};

struct SSDSpillReadResult {
    bool ok = false;
    std::string payload;
    std::string error_message;
};

class SSDSpillManager {
public:
    explicit SSDSpillManager(std::string ssd_root);

    const std::string& SsdRoot() const { return ssd_root_; }

    SSDSpillResult Spill(int32_t store_node_id,
                         const std::string& block_hash,
                         int64_t version,
                         const std::string& payload) const;

    SSDSpillReadResult Read(const std::string& evicted_path) const;

    // True if `evicted_path` is rooted under `ssd_root_` and contains no `..`.
    bool ValidatePath(const std::string& evicted_path) const;

    // Validates path safety plus Store-local file existence/readability.
    bool ValidateExistingFile(const std::string& evicted_path,
                              std::string* error_message = nullptr) const;

    // Computes the canonical path for a given block hash + version (without
    // creating the file). Useful for tests and metadata.
    std::string ComputePath(int32_t store_node_id,
                            const std::string& block_hash,
                            int64_t version) const;

private:
    std::string ssd_root_;
};

}  // namespace falconfs::kv
