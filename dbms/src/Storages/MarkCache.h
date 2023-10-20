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

#include <Common/LRUCache.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <Interpreters/AggregationCommon.h>

#include <memory>


namespace ProfileEvents
{
extern const Event MarkCacheHits;
extern const Event MarkCacheMisses;
} // namespace ProfileEvents

namespace DB
{
/// Estimate of number of bytes in cache for marks.
struct MarksWeightFunction
{
    size_t operator()(const String & key, const MarksInCompressedFile & marks) const
    {
        auto mark_memory_usage = marks.allocated_bytes(); // marksInCompressedFile
        auto cells_memory_usage = 32; // Cells struct memory cost
        auto pod_array_memory_usage = sizeof(decltype(marks)); // PODArray struct memory cost

        // 2. the memory cost of key part
        auto str_len = key.size(); // key_len
        auto key_memory_usage = sizeof(String); // String struct memory cost

        // 3. the memory cost of hash table
        auto unordered_map_memory_usage = 28; // hash table struct approximate memory cost

        // 4. the memory cost of LRUQueue
        auto list_memory_usage = sizeof(std::list<String>); // list struct memory cost

        return mark_memory_usage + cells_memory_usage + pod_array_memory_usage + str_len * 2 + key_memory_usage * 2
            + unordered_map_memory_usage + list_memory_usage;
    }
};


/** Cache of 'marks' for StorageDeltaMerge.
  * Marks is an index structure that addresses ranges in column file, corresponding to ranges of primary key.
  */
class MarkCache : public LRUCache<String, MarksInCompressedFile, std::hash<String>, MarksWeightFunction>
{
private:
    using Base = LRUCache<String, MarksInCompressedFile, std::hash<String>, MarksWeightFunction>;

public:
    explicit MarkCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes)
    {}

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::MarkCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::MarkCacheHits);

        return result.first;
    }
};

using MarkCachePtr = std::shared_ptr<MarkCache>;

} // namespace DB
