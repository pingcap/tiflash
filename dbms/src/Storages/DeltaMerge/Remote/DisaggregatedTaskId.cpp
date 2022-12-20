#include <Storages/DeltaMerge/Remote/DisaggregatedTaskId.h>

namespace DB::DM {
bool operator==(const DisaggregatedTaskId & lhs, const DisaggregatedTaskId & rhs)
{
    return lhs.mpp_task_id == rhs.mpp_task_id && lhs.executor_id == rhs.executor_id;
}
}
