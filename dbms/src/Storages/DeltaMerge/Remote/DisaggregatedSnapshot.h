#pragma once

#include <Common/nocopyable.h>
#include <Storages/DeltaMerge/File/dtpb/column_file.pb.h>
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
    }

    dtpb::DisaggregatedPhysicalTable toRemote(const DisaggregatedTaskId & task_id) const;

    DISALLOW_COPY(DisaggregatedTableReadSnapshot);

public:
    const TableID table_id;

private:
    RSOperatorPtr filter;
    // TODO: we could reduce the members in tasks
    SegmentReadTasks tasks;
};

// The read snapshot of one physical table
// This class is not thread safe
class DisaggregatedReadSnapshot
{
public:
    using TableSnapshotMap = std::unordered_map<TableID, DisaggregatedTableReadSnapshotPtr>;

    DisaggregatedReadSnapshot() = default;

    void addTask(TableID physical_table_id, DisaggregatedTableReadSnapshotPtr && task)
    {
        if (!task)
            return;
        table_snapshots.emplace(physical_table_id, std::move(task));
    }

    const TableSnapshotMap & tasks()
    {
        return table_snapshots;
    }

    DISALLOW_COPY(DisaggregatedReadSnapshot);

private:
    std::unordered_map<TableID, DisaggregatedTableReadSnapshotPtr> table_snapshots;
};


} // namespace DB::DM
