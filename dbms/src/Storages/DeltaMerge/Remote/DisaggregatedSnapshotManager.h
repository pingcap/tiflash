#pragma once

#include <Common/nocopyable.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedTaskId.h>
#include <common/types.h>

#include <memory>
#include <shared_mutex>

namespace DB::DM
{
class DisaggregatedReadSnapshot;
using DisaggregatedReadSnapshotPtr = std::shared_ptr<DisaggregatedReadSnapshot>;

class DisaggregatedSnapshotManager;
using DisaggregatedSnapshotManagerPtr = std::unique_ptr<DisaggregatedSnapshotManager>;

// DisaggregatedSnapshotManager holds all snapshot for disaggregated read tasks
// in the write node. It's a single instance holden in global_context.
class DisaggregatedSnapshotManager
{
public:
    std::tuple<bool, String> registerSnapshot(const DisaggregatedTaskId & task_id, DisaggregatedReadSnapshotPtr && snap)
    {
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

    bool unregisterSnapshot(const DisaggregatedTaskId & snap_id)
    {
        std::unique_lock lock(mtx);
        if (auto iter = snapshots.find(snap_id); iter != snapshots.end())
        {
            snapshots.erase(iter);
            return true;
        }
        return false;
    }

    DISALLOW_COPY_AND_MOVE(DisaggregatedSnapshotManager);

    DisaggregatedSnapshotManager() = default;

private:
    mutable std::shared_mutex mtx;
    std::unordered_map<DisaggregatedTaskId, DisaggregatedReadSnapshotPtr> snapshots;
};
} // namespace DB::DM
