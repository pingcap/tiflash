#include <Flash/Mpp/getMPPTaskLog.h>
#include <Flash/Mpp/getMPPTaskTracingLog.h>

namespace DB
{
LogWithPrefixPtr getMPPTaskTracingLog(const MPPTaskId & mpp_task_id)
{
    return getMPPTaskLog(tracing_log_source, mpp_task_id);
}
} // namespace DB