#include <Storages/DeltaMerge/Remote/DisaggregatedTaskId.h>
#include <kvproto/mpp.pb.h>

namespace DB::DM
{
DisaggregatedTaskId::DisaggregatedTaskId(const mpp::DisaggregatedTaskMeta & task_meta)
    : mpp_task_id(
        task_meta.start_ts(),
        task_meta.task_id(),
        /*server_id=*/0,
        task_meta.query_ts(),
        task_meta.local_query_id())
    , executor_id(task_meta.executor_id())
{
}

mpp::DisaggregatedTaskMeta DisaggregatedTaskId::toMeta() const
{
    mpp::DisaggregatedTaskMeta meta;
    meta.set_start_ts(mpp_task_id.query_id.start_ts);
    meta.set_query_ts(mpp_task_id.query_id.query_ts);
    meta.set_local_query_id(mpp_task_id.query_id.local_query_id);
    meta.set_task_id(mpp_task_id.task_id);
    meta.set_executor_id(executor_id);
    return meta;
}

bool operator==(const DisaggregatedTaskId & lhs, const DisaggregatedTaskId & rhs)
{
    return lhs.mpp_task_id == rhs.mpp_task_id && lhs.executor_id == rhs.executor_id;
}
} // namespace DB::DM
