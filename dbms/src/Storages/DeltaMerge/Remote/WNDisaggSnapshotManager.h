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
#include <Common/SharedMutexProtected.h>
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
        const DisaggReadSnapshotPtr snap;

        SnapshotWithExpireTime(DisaggReadSnapshotPtr snap_, std::chrono::seconds basic_duration_)
            : snap(std::move(snap_))
        {
            expired_at = Clock::now() + basic_duration_;
        }

        bool refreshExpiredTime(std::chrono::seconds refresh_duration)
        {
            std::lock_guard lock(mtx);
            // Only refresh when the new expiration time is later
            auto refresh_timepoint = Clock::now() + refresh_duration;
            if (refresh_timepoint > expired_at)
            {
                expired_at = refresh_timepoint;
                return true;
            }
            return false;
        }

        Timepoint getExpiredAt() const
        {
            std::lock_guard lock(mtx);
            return expired_at;
        }

    private:
        mutable std::mutex mtx;
        Timepoint expired_at;
    };

    using SnapshotWithExpireTimePtr = std::unique_ptr<SnapshotWithExpireTime>;

public:
    explicit WNDisaggSnapshotManager(BackgroundProcessingPool & bg_pool);

    ~WNDisaggSnapshotManager();

    bool registerSnapshot(
        const DisaggTaskId & task_id,
        const DisaggReadSnapshotPtr & snap,
        std::chrono::seconds duration)
    {
        return snapshots.withExclusive([&](auto & snapshots) {
            LOG_INFO(log, "Register Disaggregated Snapshot, timeout={} task_id={}", duration, task_id);

            // Since EstablishDisagg may be retried, there may be existing snapshot.
            // We replace these existing snapshot using a new one.
            snapshots[task_id] = std::make_unique<SnapshotWithExpireTime>(snap, duration);
            return true;
        });
    }

    /*
     * Get the snapshot by task_id. If `refresh_duration` contains value, the expiration time
     * will be refreshed by `refresh_duration` from now.
     * If the snapshot is not found, return nullptr.
     */
    DisaggReadSnapshotPtr getDisaggSnapshot(
        const DisaggTaskId & task_id,
        std::optional<std::chrono::seconds> refresh_duration) const;

    bool unregisterSnapshotIfEmpty(const DisaggTaskId & task_id);

    size_t getActiveSnapshotCount() const;

    DISALLOW_COPY_AND_MOVE(WNDisaggSnapshotManager);

private:
    bool unregisterSnapshot(const DisaggTaskId & task_id);

    void clearExpiredSnapshots();

private:
    SharedMutexProtected<std::unordered_map<DisaggTaskId, SnapshotWithExpireTimePtr>> snapshots;

    BackgroundProcessingPool & pool;
    BackgroundProcessingPool::TaskHandle handle;
    LoggerPtr log;
};
} // namespace DB::DM::Remote
