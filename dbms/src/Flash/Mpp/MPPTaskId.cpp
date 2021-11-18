#include <Flash/Mpp/MPPTaskId.h>
#include <fmt/core.h>

namespace DB
{
String MPPTaskId::toString() const
{
    return isUnknown() ? "<query:N/A,task:N/A>" : fmt::format("<query:{},task:{}>", start_ts, task_id);
}

const MPPTaskId MPPTaskId::unknown_mpp_task_id = MPPTaskId{};

bool operator==(const MPPTaskId & lid, const MPPTaskId & rid)
{
    return lid.start_ts == rid.start_ts && lid.task_id == rid.task_id;
}
} // namespace DB