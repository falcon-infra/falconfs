#include "vllm_kv_cache/src/metadata/lru_manager.h"

namespace falconfs::kv {

void LRUManager::AddStored(const std::string& block_hash) {
    Touch(block_hash);
}

void LRUManager::Touch(const std::string& block_hash) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = index_.find(block_hash);
    if (it != index_.end()) {
        order_.erase(it->second);
    }
    order_.push_back(block_hash);
    auto last = std::prev(order_.end());
    index_[block_hash] = last;
}

void LRUManager::Remove(const std::string& block_hash) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = index_.find(block_hash);
    if (it == index_.end()) return;
    order_.erase(it->second);
    index_.erase(it);
}

std::vector<std::string> LRUManager::ColdCandidates(std::size_t limit) const {
    std::vector<std::string> out;
    std::lock_guard<std::mutex> lock(mu_);
    out.reserve(std::min(limit, order_.size()));
    for (auto it = order_.begin(); it != order_.end() && out.size() < limit; ++it) {
        out.push_back(*it);
    }
    return out;
}

std::size_t LRUManager::Size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return order_.size();
}

}  // namespace falconfs::kv
