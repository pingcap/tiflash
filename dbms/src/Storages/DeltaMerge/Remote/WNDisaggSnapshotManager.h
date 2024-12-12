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

#include <Common/Logger.h>
#include <Common/nocopyable.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot_fwd.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/WNDisaggSnapshotManager_fwd.h>
#include <Storages/KVStore/Types.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <fmt/chrono.h>

namespace DB::DM::Remote
{
/**
 * WNDisaggSnapshotManager holds all snapshots for disaggregated read tasks
 * in the write node. It's a single instance for each TiFlash node.
 */
class WNDisaggSnapshotManager
{
public:
    struct SnapshotWithExpireTime
    {
        DisaggReadSnapshotPtr snap;
        std::chrono::seconds refresh_duration;
        std::mutex mtx;
        Timepoint expired_at;

        SnapshotWithExpireTime(DisaggReadSnapshotPtr snap_, std::chrono::seconds refresh_duration_)
            : snap(std::move(snap_))
            , refresh_duration(refresh_duration_)
        {
            refreshExpiredTime();
        }

        void refreshExpiredTime()
        {
            std::lock_guard lock(mtx);
            expired_at = Clock::now() + refresh_duration;
        }
    };

    using SnapshotWithExpireTimePtr = std::unique_ptr<SnapshotWithExpireTime>;

public:
    explicit WNDisaggSnapshotManager(BackgroundProcessingPool & bg_pool);

    ~WNDisaggSnapshotManager();

    bool registerSnapshot(
        const DisaggTaskId & task_id,
        const DisaggReadSnapshotPtr & snap,
        std::chrono::seconds refresh_duration)
    {
        std::unique_lock lock(mtx);
        LOG_INFO(log, "Register Disaggregated Snapshot, task_id={}", task_id);

        // Since EstablishDisagg may be retried, there may be existing snapshot.
        // We replace these existing snapshot using a new one.
        snapshots[task_id] = std::make_unique<SnapshotWithExpireTime>(snap, refresh_duration);
        return true;
    }

    DisaggReadSnapshotPtr getSnapshot(const DisaggTaskId & task_id, bool refresh_expiration = false) const
    {
        std::shared_lock read_lock(mtx);
        if (auto iter = snapshots.find(task_id); iter != snapshots.end())
        {
            if (refresh_expiration)
                iter->second->refreshExpiredTime();
            return iter->second->snap;
        }
        return nullptr;
    }

    bool unregisterSnapshotIfEmpty(const DisaggTaskId & task_id);

    DISALLOW_COPY_AND_MOVE(WNDisaggSnapshotManager);

private:
    bool unregisterSnapshot(const DisaggTaskId & task_id)
    {
        std::unique_lock lock(mtx);
        if (auto iter = snapshots.find(task_id); iter != snapshots.end())
        {
            LOG_INFO(log, "Unregister Disaggregated Snapshot, task_id={}", task_id);
            snapshots.erase(iter);
            return true;
        }
        return false;
    }

    void clearExpiredSnapshots();

private:
    mutable std::shared_mutex mtx;
    std::unordered_map<DisaggTaskId, SnapshotWithExpireTimePtr> snapshots;

    BackgroundProcessingPool & pool;
    BackgroundProcessingPool::TaskHandle handle;
    LoggerPtr log;
};
} // namespace DB::DM::Remote
