#pragma once

#include <map>

#include <Storages/Transaction/Types.h>

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;

struct TiKVRangeKey;
using RegionRange = std::pair<TiKVRangeKey, TiKVRangeKey>;

class KVStoreTaskLock;
class KVStore;

struct TiKVRangeKeyCmp
{
    bool operator()(const TiKVRangeKey & x, const TiKVRangeKey & y) const;
};

struct IndexNode
{
    RegionMap region_map;
};

class RegionsRangeIndex : private boost::noncopyable
{
public:
    using RootMap = std::map<TiKVRangeKey, IndexNode, TiKVRangeKeyCmp>;

    void add(const RegionPtr & new_region);

    void remove(const RegionRange & range, const RegionID region_id);

    RegionMap findByRangeOverlap(const RegionRange & range) const;

    RegionsRangeIndex();

    const RootMap & getRoot() const;

    void clear();

private:
    friend class RegionsRangeIndexKVStore;

    void tryMergeEmpty(RootMap::iterator remove_it);
    RootMap::iterator split(const TiKVRangeKey & new_start);

private:
    RootMap root;
    RootMap::const_iterator min_it;
    RootMap::const_iterator max_it;
};

class RegionsRangeIndexKVStore : RegionsRangeIndex
{
    friend class KVStore;

    void add(const RegionPtr & new_region, const KVStoreTaskLock & task_lock);
    void remove(const RegionRange & range, const RegionID region_id, const KVStoreTaskLock & task_lock);
    RegionMap findByRangeOverlap(const RegionRange & range, const KVStoreTaskLock & task_lock) const;
};

} // namespace DB
