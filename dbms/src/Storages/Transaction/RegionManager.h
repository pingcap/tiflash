#pragma once

#include <Storages/Transaction/Types.h>

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;

class RegionTaskLock;

class RegionManager : private boost::noncopyable
{
public:
    RegionTaskLock genRegionTaskLock(const RegionID region_id) const;

private:
    friend class KVStore;
    using RegionTaskElement = std::mutex;
    using RegionTaskElementPtr = std::shared_ptr<RegionTaskElement>;
    RegionTaskElementPtr getRegionTaskCtrl(const RegionID region_id) const;

    mutable std::unordered_map<RegionID, RegionTaskElementPtr> regions_ctrl;
    RegionMap regions;

    mutable std::mutex mutex;
};

class RegionTaskLock : private boost::noncopyable
{
    friend RegionTaskLock RegionManager::genRegionTaskLock(const RegionID region_id) const;

    RegionTaskLock(std::mutex & mutex_) : lock(mutex_) {}
    std::lock_guard<std::mutex> lock;
};

} // namespace DB
