#include <Flash/Mpp/MPPTaskTracingLogger.h>
#include <Flash/Mpp/getMPPTaskLog.h>

namespace DB
{
MPPTaskTracingLogger::MPPTaskTracingLogger(const MPPTaskId & mpp_task_id)
    : logger(getMPPTaskLog(tracing_log_source, mpp_task_id))
{}

void MPPTaskTracingLogger::log(const std::string & msg)
{
    logger->information(msg);
}
} // namespace DB