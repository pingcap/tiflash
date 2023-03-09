// Copyright 2023 PingCAP, Ltd.
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
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <fmt/chrono.h>

#include <memory>
#include <mutex>
#include <shared_mutex>

namespace DB
{
class Context;
}

namespace DB::DM::Remote
{
class DisaggSnapshotManager;
using DisaggSnapshotManagerPtr = std::unique_ptr<DisaggSnapshotManager>;

// DisaggSnapshotManager holds all snapshots for disaggregated read tasks
// in the write node. It's a single instance for each TiFlash node.
class DisaggSnapshotManager
{
public:
    struct SnapshotWithExpireTime
    {
        const DisaggReadSnapshotPtr snap;
        const Timepoint expired_at;
    };

public:
    explicit DisaggSnapshotManager(Context & ctx);

    ~DisaggSnapshotManager();

    bool registerSnapshot(const DisaggTaskId & task_id, DisaggReadSnapshotPtr && snap, const Timepoint & expired_at)
    {
        LOG_DEBUG(log, "Register Disaggregated Snapshot, task_id={}", task_id);

        std::unique_lock lock(mtx);
        if (auto iter = snapshots.find(task_id); iter != snapshots.end())
            return false;

        snapshots.emplace(task_id, SnapshotWithExpireTime{.snap = std::move(snap), .expired_at = expired_at});
        return true;
    }

    DisaggReadSnapshotPtr getSnapshot(const DisaggTaskId & task_id) const
    {
        std::shared_lock read_lock(mtx);
        if (auto iter = snapshots.find(task_id); iter != snapshots.end())
            return iter->second.snap;
        return nullptr;
    }

    bool unregisterSnapshot(const DisaggTaskId & task_id)
    {
        LOG_DEBUG(log, "Unregister Disaggregated Snapshot, task_id={}", task_id);

        std::unique_lock lock(mtx);
        if (auto iter = snapshots.find(task_id); iter != snapshots.end())
        {
            snapshots.erase(iter);
            return true;
        }
        return false;
    }

    void clearExpiredSnapshots();

    DISALLOW_COPY_AND_MOVE(DisaggSnapshotManager);

private:
    mutable std::shared_mutex mtx;
    std::unordered_map<DisaggTaskId, SnapshotWithExpireTime> snapshots;

    Context & global_ctx;
    BackgroundProcessingPool::TaskHandle handle;
    LoggerPtr log;
};
} // namespace DB::DM::Remote
