// LRU manager for STORED KV blocks.
//
// Tracks recency of access for blocks in the STORED state. The cold end of the
// list seeds eviction candidate selection. ALLOCATED, EVICTING, EVICTED, and
// FAILED are not normal LRU members.
#pragma once

#include <cstddef>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace falconfs::kv {

class LRUManager {
public:
    void AddStored(const std::string& block_hash);
    void Touch(const std::string& block_hash);
    void Remove(const std::string& block_hash);

    std::vector<std::string> ColdCandidates(std::size_t limit) const;
    std::size_t Size() const;

private:
    mutable std::mutex mu_;
    std::list<std::string> order_;
    std::unordered_map<std::string, std::list<std::string>::iterator> index_;
};

}  // namespace falconfs::kv
