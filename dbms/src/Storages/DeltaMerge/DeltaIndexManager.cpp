#include <Storages/DeltaMerge/DeltaIndexManager.h>

namespace CurrentMetrics
{
extern const Metric DT_DeltaIndexCacheSize;
} // namespace CurrentMetrics

namespace DB
{
namespace DM
{

void DeltaIndexManager::onRemove(const Holder & holder)
{
    if (auto p = holder.index.lock(); p)
    {
        LOG_TRACE(log, String(__FUNCTION__) << "Free DeltaIndex, [size " << p->getBytes() << "]");
        p->clear();
    }
}

void DeltaIndexManager::removeOverflow()
{
    size_t queue_size = index_map.size();
    while ((current_size > max_size) && (queue_size > 1))
    {
        const auto & id = lru_queue.front();

        auto it = index_map.find(id);
        if (it == index_map.end())
            throw Exception(String(__FUNCTION__) + " inconsistent", ErrorCodes::LOGICAL_ERROR);

        const auto & holder = it->second;

        current_size -= holder.size;

        index_map.erase(it);
        lru_queue.pop_front();
        --queue_size;

        onRemove(holder);
    }

    if (current_size > (1ull << 63))
    {
        throw Exception(String(__FUNCTION__) + " inconsistent", ErrorCodes::LOGICAL_ERROR);
    }
}

void DeltaIndexManager::refreshRef(const DeltaIndexPtr & index)
{
    if (max_size == 0)
        return;

    std::lock_guard lock(mutex);

    auto id  = index->getId();
    auto res = index_map.emplace(std::piecewise_construct, std::forward_as_tuple(id), std::forward_as_tuple());

    Holder & holder   = res.first->second;
    bool     inserted = res.second;

    if (inserted)
    {
        holder.queue_it = lru_queue.insert(lru_queue.end(), id);
    }
    else
    {
        current_size -= holder.size;
        lru_queue.splice(lru_queue.end(), lru_queue, holder.queue_it);
    }

    holder.index = index;
    holder.size  = index->getBytes();
    current_size += holder.size;

    removeOverflow();

    CurrentMetrics::set(CurrentMetrics::DT_DeltaIndexCacheSize, current_size);
}

void DeltaIndexManager::deleteRef(const DeltaIndexPtr & index)
{
    if (max_size == 0)
        return;

    std::lock_guard lock(mutex);

    auto it = index_map.find(index->getId());
    if (it == index_map.end())
        return;
    Holder & holder = it->second;
    index_map.erase(it);
    lru_queue.erase(holder.queue_it);
    current_size -= holder.size;

    CurrentMetrics::set(CurrentMetrics::DT_DeltaIndexCacheSize, current_size);
}

DeltaIndexPtr DeltaIndexManager::getRef(UInt64 index_id)
{
    if (max_size == 0)
        return {};
    std::lock_guard lock(mutex);

    auto it = index_map.find(index_id);
    if (it == index_map.end())
        return {};
    Holder & holder = it->second;
    return holder.index.lock();
}

} // namespace DM
} // namespace DB