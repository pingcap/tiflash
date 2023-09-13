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

#include <Storages/KVStore/MultiRaft/RegionsRangeIndex.h>
#include <Storages/KVStore/Types.h>
#include <Storages/KVStore/Utils.h>

#include <unordered_map>

namespace DB
{
class RegionTaskLock;

struct RegionTaskCtrl : MutexLockWrap
{
    using Mut = std::mutex;
    /// The life time of each RegionTaskElement element should be as long as RegionManager, just return const ref.
    struct RegionTaskElement : private boost::noncopyable
    {
        mutable Mut mutex;
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

    RegionReadLock genReadLock() const { return {genSharedLock(), regions, region_range_index}; }

    RegionWriteLock genWriteLock() { return {genUniqueLock(), regions, region_range_index}; }

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

    explicit RegionTaskLock(RegionTaskCtrl::Mut & mutex_)
        : lock(mutex_)
    {}
    std::lock_guard<RegionTaskCtrl::Mut> lock;
};

} // namespace DB
