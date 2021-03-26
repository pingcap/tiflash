#pragma once

#include <Storages/Transaction/Types.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;

class RegionTaskLock;

/// RegionManager is used to store region instance and mutex for region to execute raft cmd/task.
class RegionManager : private boost::noncopyable
{
public:
    /// Encapsulate the task lock for region
    RegionTaskLock genRegionTaskLock(const RegionID region_id) const;

private:
    friend class KVStore;

    struct RegionTaskElement : private boost::noncopyable
    {
        mutable std::mutex mutex;
    };

    /// The life time of each RegionTaskElement element should be as long as RegionManager, just return const ref.
    const RegionTaskElement & getRegionTaskCtrl(const RegionID region_id) const;

    /// RegionManager can only be constructed by KVStore.
    RegionManager() = default;

private:
    mutable std::unordered_map<RegionID, RegionTaskElement> regions_ctrl;
    RegionMap regions;
    mutable std::mutex mutex;
};

/// Task lock for region to prevent other thread persist middle state during applying raft cmd.
class RegionTaskLock : private boost::noncopyable
{
    friend RegionTaskLock RegionManager::genRegionTaskLock(const RegionID region_id) const;

    RegionTaskLock(std::mutex & mutex_) : lock(mutex_) {}
    std::lock_guard<std::mutex> lock;
};

} // namespace DB
