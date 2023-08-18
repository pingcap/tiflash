// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Storages/DeltaMerge/DeltaIndexManager.h>
#include <Storages/DeltaMerge/Segment.h>

namespace CurrentMetrics
{
extern const Metric DT_DeltaIndexCacheSize;
} // namespace CurrentMetrics

namespace DB
{
namespace DM
{
void DeltaIndexManager::removeOverflow(std::vector<DeltaIndexPtr> & removed)
{
    size_t queue_size = index_map.size();
    while ((current_size > max_size) && (queue_size > 1))
    {
        const auto & id = lru_queue.front();

        auto it = index_map.find(id);
        if (it == index_map.end())
            throw Exception(String(__FUNCTION__) + " inconsistent", ErrorCodes::LOGICAL_ERROR);

        const Holder & holder = it->second;
        if (auto p = holder.index.lock(); p)
        {
            LOG_TRACE(log, "Free DeltaIndex, [size {}]", p->getBytes());

            // We put the evicted index into removed list, and free them later.
            auto tmp = std::make_shared<DeltaIndex>();
            p->swap(*tmp);
            removed.push_back(tmp);
        }

        current_size -= holder.size;
        --queue_size;
        lru_queue.pop_front();
        // Remove it later
        index_map.erase(it);
    }

    if (current_size > (1ull << 63))
    {
        throw Exception(String(__FUNCTION__) + " inconsistent, current_size < 0", ErrorCodes::LOGICAL_ERROR);
    }
}

void DeltaIndexManager::refreshRef(const DeltaIndexPtr & index)
{
    if (max_size == 0)
        return;

    // Don't free the removed DeltaIndexs inside the lock scope.
    // Instead, use a removed list to actually free them in current thread, so that we don't block other threads.
    std::vector<DeltaIndexPtr> removed;

    {
        std::lock_guard lock(mutex);

        auto id = index->getId();
        auto res = index_map.emplace(std::piecewise_construct, std::forward_as_tuple(id), std::forward_as_tuple());

        Holder & holder = res.first->second;
        bool inserted = res.second;

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
        holder.size = index->getBytes();
        current_size += holder.size;

        removeOverflow(removed);
        CurrentMetrics::set(CurrentMetrics::DT_DeltaIndexCacheSize, current_size);
    }
}

void DeltaIndexManager::deleteRef(const DeltaIndexPtr & index)
{
    if (max_size == 0)
        return;

    DeltaIndex empty;

    {
        std::lock_guard lock(mutex);

        auto it = index_map.find(index->getId());
        if (it == index_map.end())
            return;

        Holder & holder = it->second;
        if (auto p = holder.index.lock(); p)
        {
            LOG_TRACE(log, "Free DeltaIndex, [size {}]", p->getBytes());

            // Free it out of lock scope.
            p->swap(empty);
        }

        current_size -= holder.size;
        lru_queue.erase(holder.queue_it);
        // Remove it later
        index_map.erase(it);
        CurrentMetrics::set(CurrentMetrics::DT_DeltaIndexCacheSize, current_size);
    }
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