#include <Storages/DeltaMerge/Remote/DisaggregatedSnapshot.h>
#include <Storages/DeltaMerge/Segment.h>
#include <common/logger_useful.h>
#include <kvproto/mpp.pb.h>

#include <memory>

namespace DB::DM
{

dtpb::DisaggregatedPhysicalTable
DisaggregatedTableReadSnapshot::toRemote(const DisaggregatedTaskId & task_id) const
{
    dtpb::DisaggregatedPhysicalTable remote_table;
    remote_table.set_snapshot_id(task_id.toMeta().SerializeAsString());
    remote_table.set_table_id(table_id);
    for (const auto & seg_task : tasks)
    {
        auto remote_seg = seg_task->read_snapshot->serializeToRemoteProtocol(
            seg_task->segment->segmentId(),
            seg_task->segment->getRowKeyRange(),
            /*read_ranges*/ seg_task->ranges);
        remote_table.mutable_segments()->Add(std::move(remote_seg));
    }
    return remote_table;
}


SegmentPagesFetchTask DisaggregatedReadSnapshot::popSegTask(TableID physical_table_id, UInt64 segment_id)
{
    std::unique_lock lock(mtx);
    auto table_iter = table_snapshots.find(physical_table_id);
    if (table_iter == table_snapshots.end())
    {
        return SegmentPagesFetchTask::error(fmt::format("Segment task not found by table_id, table_id={}, segment_id={}", physical_table_id, segment_id));
    }

    assert(table_iter->second->table_id == physical_table_id);
    auto seg_task = table_iter->second->popTask(segment_id);
    if (!seg_task)
    {
        return SegmentPagesFetchTask::error(fmt::format("Segment task not found by segment_id, table_id={}, segment_id={}", physical_table_id, segment_id));
    }

    auto task = SegmentPagesFetchTask::task(
        seg_task,
        table_iter->second->column_defines,
        table_iter->second->output_field_types);
    if (table_iter->second->empty())
    {
        table_snapshots.erase(table_iter);
        LOG_DEBUG(Logger::get(), "all tasks of table are pop, table_id={}", physical_table_id);
    }
    return task;
}

bool DisaggregatedReadSnapshot::empty() const
{
    for (const auto & tbl : table_snapshots)
    {
        if (!tbl.second->empty())
            return false;
    }
    return true;
}

SegmentReadTaskPtr DisaggregatedTableReadSnapshot::popTask(const UInt64 segment_id)
{
    std::unique_lock lock(mtx);
    for (auto iter = tasks.begin(); iter != tasks.end(); ++iter)
    {
        auto seg = *iter;
        if (seg->segment->segmentId() == segment_id)
        {
            tasks.erase(iter);
            return seg;
        }
    }
    return nullptr;
}

} // namespace DB::DM
