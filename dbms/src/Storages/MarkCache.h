// Copyright 2022 PingCAP, Ltd.
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
    size_t operator()(const MarksInCompressedFile & marks) const
    {
        return marks.allocated_bytes();
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
