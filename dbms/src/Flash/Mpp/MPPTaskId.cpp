#include <Flash/Mpp/MPPTaskId.h>
#include <fmt/core.h>

namespace DB
{
bool MPPTaskId::operator<(const MPPTaskId & rhs) const
{
    return start_ts < rhs.start_ts || (start_ts == rhs.start_ts && task_id < rhs.task_id);
}

String MPPTaskId::toString() const
{
    return task_id == unknown_task_id ? "<query:N/A,task:N/A>" : fmt::format("<query:{},task:{}>", start_ts, task_id);
}

const MPPTaskId MPPTaskId::empty_mpp_task_id = MPPTaskId{};
} // namespace DB