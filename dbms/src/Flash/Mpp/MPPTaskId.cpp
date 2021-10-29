#include <Flash/Mpp/MPPTaskId.h>
#include <fmt/core.h>

namespace DB
{
String MPPTaskId::toString() const
{
    return fmt::format("[{},{}]", start_ts, task_id);
}
} // namespace DB