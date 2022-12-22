#pragma once

#include <Common/nocopyable.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/dtpb/column_file.pb.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedTaskId.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/Transaction/Types.h>
#include <tipb/expression.pb.h>

#include <mutex>
#include <unordered_map>

namespace DB::DM
{

class DisaggregatedTableReadSnapshot;
using DisaggregatedTableReadSnapshotPtr = std::unique_ptr<DisaggregatedTableReadSnapshot>;
class DisaggregatedReadSnapshot;
using DisaggregatedReadSnapshotPtr = std::shared_ptr<DisaggregatedReadSnapshot>;

struct SegmentPagesFetchTask
{
    SegmentReadTaskPtr seg_task;
    DM::ColumnDefinesPtr column_defines;
    std::shared_ptr<std::vector<tipb::FieldType>> output_field_types;

    String err_msg;

    bool isValid() const { return seg_task != nullptr; }

public:
    static SegmentPagesFetchTask error(String err_msg)
    {
        return SegmentPagesFetchTask{nullptr, nullptr, nullptr, std::move(err_msg)};
    }
    static SegmentPagesFetchTask task(
        SegmentReadTaskPtr seg_task,
        DM::ColumnDefinesPtr column_defines,
        std::shared_ptr<std::vector<tipb::FieldType>> output_field_types)
    {
        return SegmentPagesFetchTask{
            std::move(seg_task),
            std::move(column_defines),
            std::move(output_field_types),
            ""};
    }
};

// The read snapshot stored on the write node.
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

    SegmentPagesFetchTask popSegTask(TableID physical_table_id, UInt64 segment_id);

    bool empty() const;
    const TableSnapshotMap & tableSnapshots() const { return table_snapshots; }

    DISALLOW_COPY(DisaggregatedReadSnapshot);

private:
    mutable std::mutex mtx;
    TableSnapshotMap table_snapshots;
};

// The read snapshot of one physical table
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

    SegmentReadTaskPtr popTask(UInt64 segment_id);

    bool empty() const { return tasks.empty(); }

    DISALLOW_COPY(DisaggregatedTableReadSnapshot);

    const SegmentReadTasks & getTasks() const { return tasks; }

public:
    const TableID table_id;

    // TODO: these can be shared on DisaggregatedReadSnapshot level
    DM::ColumnDefinesPtr column_defines;
    std::shared_ptr<std::vector<tipb::FieldType>> output_field_types;

private:
    mutable std::mutex mtx;
    RSOperatorPtr filter;
    // TODO: we could reduce the members in tasks
    SegmentReadTasks tasks;
};


} // namespace DB::DM
