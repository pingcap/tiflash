#pragma once

#include <memory>

#include <Common/LRUCache.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Interpreters/AggregationCommon.h>
#include <DataStreams/MarkInCompressedFile.h>


namespace ProfileEvents
{
    extern const Event MarkCacheHits;
    extern const Event MarkCacheMisses;
}

namespace DB
{

/// Estimate of number of bytes in cache for marks.
struct MarksWeightFunction
{
    size_t operator()(const MarksInCompressedFile & marks) const
    {
        /// NOTE Could add extra 100 bytes for overhead of std::vector, cache structures and allocator.
        return marks.size() * sizeof(MarkInCompressedFile);
    }
};


/** Cache of 'marks' for StorageMergeTree.
  * Marks is an index structure that addresses ranges in column file, corresponding to ranges of primary key.
  */
class MarkCache : public LRUCache<String, MarksInCompressedFile, std::hash<String>, MarksWeightFunction>
{
private:
    using Base = LRUCache<String, MarksInCompressedFile, std::hash<String>, MarksWeightFunction>;

public:
    MarkCache(size_t max_size_in_bytes, const Delay & expiration_delay)
        : Base(max_size_in_bytes, expiration_delay) {}

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

}
