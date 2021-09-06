#pragma once

#include <Storages/DeltaMerge/DeltaIndex.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DM
{
/// This class mange the life time of DeltaIndies in memory.
/// It will free the most rarely used DeltaIndex when the total memory usage exceeds the threshold.
class DeltaIndexManager
{
private:
    // Note that we don't use Common/LRUCache.h here. Because by using it, we cannot implement the correct logic of
    // when to call DeltaIndex::clear().

    struct Holder;
    using IndexMap = std::unordered_map<UInt64, Holder>;
    using LRUQueue = std::list<UInt64>;
    using LRUQueueItr = typename LRUQueue::iterator;

    using WeakIndexPtr = std::weak_ptr<DeltaIndex>;

    struct Holder
    {
        WeakIndexPtr index;
        size_t size;
        LRUQueueItr queue_it;
    };

private:
    IndexMap index_map;
    LRUQueue lru_queue;

    size_t current_size = 0;
    const size_t max_size;

    Poco::Logger * log;

    std::mutex mutex;

private:
    void removeOverflow(std::vector<DeltaIndexPtr> & removed);

public:
    explicit DeltaIndexManager(size_t max_size_)
        : max_size(max_size_)
        , log(&Poco::Logger::get("DeltaIndexManager"))
    {}

    /// Note that if isLimit() is false, than this method always return 0.
    size_t currentSize() { return current_size; }

    bool isLimit() { return max_size != 0; }

    /// Put the reference of DeltaIndex into this manager.
    void refreshRef(const DeltaIndexPtr & index);

    /// Remove the reference of DeltaIndex from this manager.
    /// Note that this method will NOT cause the the specific DeltaIndex to be freed.
    void deleteRef(const DeltaIndexPtr & index);

    /// Try to get the DeltaIndex from this manager. Return empty if not found.
    /// Used by test cases.
    DeltaIndexPtr getRef(UInt64 index_id);
};

using DeltaIndexManagerPtr = std::shared_ptr<DeltaIndexManager>;

} // namespace DM

} // namespace DB
