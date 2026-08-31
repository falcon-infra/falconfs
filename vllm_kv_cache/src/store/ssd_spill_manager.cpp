#include "vllm_kv_cache/src/store/ssd_spill_manager.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <utility>

namespace falconfs::kv {

namespace {

constexpr const char* kHexChars = "0123456789abcdef";

std::string HexEncode(const std::string& bytes) {
    std::string out;
    out.resize(bytes.size() * 2);
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        unsigned char c = static_cast<unsigned char>(bytes[i]);
        out[2 * i + 0] = kHexChars[c >> 4];
        out[2 * i + 1] = kHexChars[c & 0xF];
    }
    return out;
}

bool MakeDirP(const std::string& path) {
    std::string acc;
    for (std::size_t i = 0; i < path.size(); ++i) {
        if (path[i] == '/') {
            if (i == 0) {
                acc.push_back('/');
                continue;
            }
            if (!acc.empty() && acc != "/") {
                if (::mkdir(acc.c_str(), 0700) != 0 && errno != EEXIST) {
                    return false;
                }
            }
            acc.push_back('/');
        } else {
            acc.push_back(path[i]);
        }
    }
    if (!acc.empty() && acc != "/") {
        if (::mkdir(acc.c_str(), 0700) != 0 && errno != EEXIST) {
            return false;
        }
    }
    return true;
}

bool ContainsTraversal(const std::string& s) {
    return s.find("/..") != std::string::npos || s.rfind("../", 0) == 0 || s == "..";
}

}  // namespace

SSDSpillManager::SSDSpillManager(std::string ssd_root) : ssd_root_(std::move(ssd_root)) {
    while (ssd_root_.size() > 1 && ssd_root_.back() == '/') {
        ssd_root_.pop_back();
    }
}

std::string SSDSpillManager::ComputePath(int32_t store_node_id,
                                         const std::string& block_hash,
                                         int64_t version) const {
    const std::string hex = HexEncode(block_hash);
    const std::string prefix = hex.size() >= 4 ? hex.substr(0, 4) : hex;
    std::ostringstream oss;
    oss << ssd_root_ << '/' << store_node_id << '/' << prefix << '/' << hex << '.' << version << ".kv";
    return oss.str();
}

bool SSDSpillManager::ValidatePath(const std::string& evicted_path) const {
    if (ssd_root_.empty() || evicted_path.empty()) return false;
    if (evicted_path.front() != '/') return false;
    if (ContainsTraversal(evicted_path)) return false;
    if (evicted_path.size() <= ssd_root_.size()) return false;
    if (evicted_path.compare(0, ssd_root_.size(), ssd_root_) != 0) return false;
    if (evicted_path[ssd_root_.size()] != '/') return false;
    return true;
}


bool SSDSpillManager::ValidateExistingFile(const std::string& evicted_path,
                                           std::string* error_message) const {
    auto set_err = [&](const std::string& msg) {
        if (error_message != nullptr) *error_message = msg;
        return false;
    };
    if (!ValidatePath(evicted_path)) {
        return set_err("invalid evicted_path");
    }
    struct stat st;
    if (::stat(evicted_path.c_str(), &st) != 0) {
        return set_err(std::string("stat failed: ") + std::strerror(errno));
    }
    if (!S_ISREG(st.st_mode)) {
        return set_err("evicted_path is not a regular file");
    }
    int fd = ::open(evicted_path.c_str(), O_RDONLY);
    if (fd < 0) {
        return set_err(std::string("open failed: ") + std::strerror(errno));
    }
    ::close(fd);
    if (error_message != nullptr) error_message->clear();
    return true;
}

SSDSpillResult SSDSpillManager::Spill(int32_t store_node_id,
                                      const std::string& block_hash,
                                      int64_t version,
                                      const std::string& payload) const {
    SSDSpillResult result;
    if (ssd_root_.empty()) {
        result.error_message = "ssd_root not configured";
        return result;
    }
    if (block_hash.empty()) {
        result.error_message = "empty block_hash";
        return result;
    }

    const std::string final_path = ComputePath(store_node_id, block_hash, version);
    if (!ValidatePath(final_path)) {
        result.error_message = "computed path failed validation";
        return result;
    }

    const std::string parent = final_path.substr(0, final_path.find_last_of('/'));
    if (!MakeDirP(parent)) {
        result.error_message = "mkdir failed: " + parent;
        return result;
    }

    int fd = ::open(final_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        result.error_message = std::string("open failed: ") + std::strerror(errno);
        return result;
    }

    const char* data = payload.data();
    std::size_t left = payload.size();
    while (left > 0) {
        ssize_t n = ::write(fd, data, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            ::unlink(final_path.c_str());
            result.error_message = std::string("write failed: ") + std::strerror(errno);
            return result;
        }
        data += n;
        left -= static_cast<std::size_t>(n);
    }

#if defined(__linux__)
    if (::fdatasync(fd) != 0) {
        ::close(fd);
        result.error_message = std::string("fdatasync failed: ") + std::strerror(errno);
        return result;
    }
#else
    if (::fsync(fd) != 0) {
        ::close(fd);
        result.error_message = std::string("fsync failed: ") + std::strerror(errno);
        return result;
    }
#endif

    if (::close(fd) != 0) {
        result.error_message = std::string("close failed: ") + std::strerror(errno);
        return result;
    }

    result.ok = true;
    result.evicted_path = final_path;
    return result;
}

SSDSpillReadResult SSDSpillManager::Read(const std::string& evicted_path) const {
    SSDSpillReadResult result;
    if (!ValidatePath(evicted_path)) {
        result.error_message = "invalid evicted_path";
        return result;
    }
    std::ifstream f(evicted_path, std::ios::binary);
    if (!f) {
        result.error_message = "open failed: " + evicted_path;
        return result;
    }
    std::ostringstream oss;
    oss << f.rdbuf();
    result.ok = true;
    result.payload = oss.str();
    return result;
}

}  // namespace falconfs::kv
