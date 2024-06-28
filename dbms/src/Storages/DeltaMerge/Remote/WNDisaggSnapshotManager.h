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
        DisaggReadSnapshotPtr snap;
        Timepoint expired_at;
    };

public:
    explicit WNDisaggSnapshotManager(BackgroundProcessingPool & bg_pool);

    ~WNDisaggSnapshotManager();

    bool registerSnapshot(
        const DisaggTaskId & task_id,
        const DisaggReadSnapshotPtr & snap,
        const Timepoint & expired_at)
    {
        return snapshots.withExclusive([&](auto & snapshots) {
            LOG_INFO(log, "Register Disaggregated Snapshot, task_id={}", task_id);

            // Since EstablishDisagg may be retried, there may be existing snapshot.
            // We replace these existing snapshot using a new one.
            snapshots[task_id] = SnapshotWithExpireTime{.snap = snap, .expired_at = expired_at};
            return true;
        });
    }

    DisaggReadSnapshotPtr getSnapshot(const DisaggTaskId & task_id) const
    {
        return snapshots.withShared([&](auto & snapshots) {
            if (auto iter = snapshots.find(task_id); iter != snapshots.end())
                return iter->second.snap;
            return DisaggReadSnapshotPtr{nullptr};
        });
    }

    bool unregisterSnapshotIfEmpty(const DisaggTaskId & task_id);

    DISALLOW_COPY_AND_MOVE(WNDisaggSnapshotManager);

private:
    bool unregisterSnapshot(const DisaggTaskId & task_id)
    {
        return snapshots.withExclusive([&](auto & snapshots) {
            if (auto iter = snapshots.find(task_id); iter != snapshots.end())
            {
                LOG_INFO(log, "Unregister Disaggregated Snapshot, task_id={}", task_id);
                snapshots.erase(iter);
                return true;
            }
            return false;
        });
    }

    void clearExpiredSnapshots();

private:
    SharedMutexProtected<std::unordered_map<DisaggTaskId, SnapshotWithExpireTime>> snapshots;

    BackgroundProcessingPool & pool;
    BackgroundProcessingPool::TaskHandle handle;
    LoggerPtr log;
};
} // namespace DB::DM::Remote
