#include <Storages/DeltaMerge/Remote/DisaggregatedSnapshot.h>
#include <Storages/DeltaMerge/Segment.h>
#include <kvproto/mpp.pb.h>

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
        auto remote_seg = seg_task->read_snapshot->toRemote(
            seg_task->segment->segmentId(),
            seg_task->segment->getRowKeyRange());
        remote_table.mutable_segments()->Add(std::move(remote_seg));
    }
    return remote_table;
}

} // namespace DB::DM
