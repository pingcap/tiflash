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

    LoggerPtr log;

    std::mutex mutex;

private:
    void removeOverflow(std::vector<DeltaIndexPtr> & removed);

public:
    explicit DeltaIndexManager(size_t max_size_)
        : max_size(max_size_)
        , log(Logger::get())
    {}

    /// Note that if isLimit() is false, than this method always return 0.
    size_t currentSize() const { return current_size; }

    bool isLimit() const { return max_size != 0; }

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
