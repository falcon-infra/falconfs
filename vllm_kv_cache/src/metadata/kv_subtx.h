// KV sub-transaction utility (v6 §4.1.2, §4.5).
//
// Mutating handlers in the production PG-extension path must process items in
// sub-batches of `BATCH_OPERATION_GROUP_SIZE = 8` wrapped by
// `BeginInternalSubTransaction()` / `ReleaseCurrentSubTransaction()`. Tests
// and reference flows do not need real PG transactions; they use the no-op
// implementation below. The PG extension can supply a `PgSubTransaction`
// override that calls the real PG APIs.
#pragma once

#include <cstddef>
#include <vector>

namespace falconfs::kv {

// `BATCH_OPERATION_GROUP_SIZE = 8` mirrors the existing FalconFS constant in
// `falcon/metadb/meta_handle.c:34`.
constexpr int kBatchOperationGroupSize = 8;

class KVSubTransaction {
public:
    virtual ~KVSubTransaction() = default;
    // Open a new sub-transaction. Required before any mutating catalog access.
    virtual void Begin() = 0;
    // Successfully release the current sub-transaction. After Commit() the
    // sub-batch's writes are visible to the rest of the parent transaction.
    virtual void Commit() = 0;
    // Roll back and release the current sub-transaction. After Rollback() the
    // sub-batch's writes are discarded.
    virtual void Rollback() = 0;
};

class NoopKVSubTransaction : public KVSubTransaction {
public:
    void Begin() override { ++begin_count_; }
    void Commit() override { ++commit_count_; }
    void Rollback() override { ++rollback_count_; }

    int BeginCount() const { return begin_count_; }
    int CommitCount() const { return commit_count_; }
    int RollbackCount() const { return rollback_count_; }

private:
    int begin_count_ = 0;
    int commit_count_ = 0;
    int rollback_count_ = 0;
};

// Process `items` in sub-batches of `group_size`, calling `Begin` before each
// chunk and `Commit` (or `Rollback` if `chunk_fn` throws) after each chunk.
// Returns the number of chunks committed. Useful as a thin wrapper around
// production sub-transaction loops.
template <typename Item, typename ChunkFn>
std::size_t ProcessInSubBatches(const std::vector<Item>& items,
                                std::size_t group_size,
                                KVSubTransaction* tx,
                                ChunkFn chunk_fn) {
    if (group_size == 0) group_size = kBatchOperationGroupSize;
    std::size_t committed = 0;
    for (std::size_t i = 0; i < items.size(); i += group_size) {
        std::size_t end = std::min(i + group_size, items.size());
        tx->Begin();
        try {
            for (std::size_t j = i; j < end; ++j) {
                chunk_fn(items[j]);
            }
            tx->Commit();
            ++committed;
        } catch (...) {
            tx->Rollback();
            throw;
        }
    }
    return committed;
}

}  // namespace falconfs::kv
