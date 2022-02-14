#pragma once

#include <Storages/Transaction/RegionsRangeIndex.h>
#include <Storages/Transaction/Types.h>
#include <Storages/Transaction/Utils.h>

#include <unordered_map>

namespace DB
{
class RegionTaskLock;

struct RegionTaskCtrl : MutexLockWrap
{
    /// The life time of each RegionTaskElement element should be as long as RegionManager, just return const ref.
    struct RegionTaskElement : private boost::noncopyable
    {
        mutable std::mutex mutex;
    };
    /// Encapsulate the task lock for region
    RegionTaskLock genRegionTaskLock(RegionID region_id) const;

private:
    mutable std::unordered_map<RegionID, RegionTaskElement> regions;
};

/// RegionManager is used to store region instance and mutex for region to execute raft cmd/task.
struct RegionManager : SharedMutexLockWrap
{
    struct RegionReadLock
    {
        std::shared_lock<std::shared_mutex> lock;
        const RegionMap & regions;
        const RegionsRangeIndex & index;
    };

    struct RegionWriteLock
    {
        std::unique_lock<std::shared_mutex> lock;
        RegionMap & regions;
        RegionsRangeIndex & index;
    };

    RegionReadLock genRegionReadLock() const
    {
        return {genReadLockGuard(), regions, region_range_index};
    }

    RegionWriteLock genRegionWriteLock()
    {
        return {genWriteLockGuard(), regions, region_range_index};
    }

    /// Encapsulate the task lock for region
    RegionTaskLock genRegionTaskLock(RegionID region_id) const;

    /// RegionManager can only be constructed by KVStore.
    RegionManager() = default;

private:
    RegionTaskCtrl region_task_ctrl;
    RegionMap regions;
    // region_range_index must be protected by task_mutex. It's used to search for region by range.
    // region merge/split/apply-snapshot/remove will change the range.
    RegionsRangeIndex region_range_index;
};

/// Task lock for region to prevent other thread persist middle state during applying raft cmd.
class RegionTaskLock : private boost::noncopyable
{
    friend struct RegionTaskCtrl;

    explicit RegionTaskLock(std::mutex & mutex_)
        : lock(mutex_)
    {}
    std::lock_guard<std::mutex> lock;
};

} // namespace DB
