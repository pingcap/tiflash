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
#include <Storages/DeltaMerge/Remote/DisaggregatedSnapshot_fwd.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedTaskId.h>
#include <common/logger_useful.h>
#include <common/types.h>

#include <memory>
#include <shared_mutex>

namespace DB::DM::Remote
{
class DisaggregatedSnapshotManager;
using DisaggregatedSnapshotManagerPtr = std::unique_ptr<DisaggregatedSnapshotManager>;

// DisaggregatedSnapshotManager holds all snapshots for disaggregated read tasks
// in the write node. It's a single instance for each TiFlash node.
class DisaggregatedSnapshotManager
{
public:
    DisaggregatedSnapshotManager()
        : log(Logger::get())
    {}

    std::tuple<bool, String> registerSnapshot(const DisaggregatedTaskId & task_id, DisaggregatedReadSnapshotPtr && snap)
    {
        LOG_DEBUG(log, "Register Disaggregated Snapshot, task_id={}", task_id);

        std::unique_lock lock(mtx);
        if (auto iter = snapshots.find(task_id); iter != snapshots.end())
            return {false, "disaggregated task has been registered"};

        snapshots.emplace(task_id, std::move(snap));
        return {true, ""};
    }

    DisaggregatedReadSnapshotPtr getSnapshot(const DisaggregatedTaskId & task_id) const
    {
        std::shared_lock read_lock(mtx);
        if (auto iter = snapshots.find(task_id); iter != snapshots.end())
            return iter->second;
        return nullptr;
    }

    bool unregisterSnapshot(const DisaggregatedTaskId & task_id)
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

    DISALLOW_COPY_AND_MOVE(DisaggregatedSnapshotManager);

private:
    mutable std::shared_mutex mtx;
    std::unordered_map<DisaggregatedTaskId, DisaggregatedReadSnapshotPtr> snapshots;
    LoggerPtr log;
};
} // namespace DB::DM::Remote
