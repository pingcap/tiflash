#pragma once

#include <Common/nocopyable.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedTaskId.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/Transaction/Types.h>

#include <mutex>
#include <unordered_map>

namespace DB::DM
{

class DisaggregatedTableReadSnapshot;
using DisaggregatedTableReadSnapshotPtr = std::unique_ptr<DisaggregatedTableReadSnapshot>;
class DisaggregatedReadSnapshot;
using DisaggregatedReadSnapshotPtr = std::shared_ptr<DisaggregatedReadSnapshot>;

class DisaggregatedTableReadSnapshot
{
public:
    DisaggregatedTableReadSnapshot(TableID table_id_, RSOperatorPtr filter_, SegmentReadTasks && tasks_)
        : table_id(table_id_)
        , filter(std::move(filter_))
        , tasks(std::move(tasks_))
    {
        UNUSED(table_id);
    }

private:
    const TableID table_id;
    RSOperatorPtr filter;
    // TODO: we could reduce the members in tasks
    SegmentReadTasks tasks;
};

class DisaggregatedReadSnapshot
{
public:
    void addTask(TableID physical_table_id, DisaggregatedTableReadSnapshotPtr && task)
    {
        if (!task)
            return;
        table_snapshots.emplace(physical_table_id, std::move(task));
    }

private:
    std::unordered_map<TableID, DisaggregatedTableReadSnapshotPtr> table_snapshots;
};

class DisaggregatedSnapshotManager;
using DisaggregatedSnapshotManagerPtr = std::unique_ptr<DisaggregatedSnapshotManager>;
// DisaggregatedSnapshotManager holds all snapshot for disaggregated read tasks
// in the write node. It's a single instance holden in global_context.
class DisaggregatedSnapshotManager
{
public:
    std::tuple<bool, String> registerSnapshot(const DisaggregatedTaskId & task_id, DisaggregatedReadSnapshotPtr && snap)
    {
        if (auto iter = snapshots.find(task_id); iter != snapshots.end())
            return {false, "disaggregated task has been registered"};

        snapshots.emplace(task_id, std::move(snap));
        return {true, ""};
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
    std::mutex mtx;
    std::unordered_map<DisaggregatedTaskId, DisaggregatedReadSnapshotPtr> snapshots;
};


} // namespace DB::DM
