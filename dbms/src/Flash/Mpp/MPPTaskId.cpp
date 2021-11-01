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
    return fmt::format("[{},{}]", start_ts, task_id);
}
} // namespace DB