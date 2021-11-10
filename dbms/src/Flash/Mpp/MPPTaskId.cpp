#include <Flash/Mpp/MPPTaskId.h>
#include <fmt/core.h>

namespace DB
{
String MPPTaskId::toString() const
{
    return isUnknown() ? "<query:N/A,task:N/A>" : fmt::format("<query:{},task:{}>", start_ts, task_id);
}

const MPPTaskId MPPTaskId::unknown_mpp_task_id = MPPTaskId{};
} // namespace DB