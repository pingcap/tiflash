#include <Flash/Mpp/getMPPTaskLog.h>

namespace DB
{
LogWithPrefixPtr getMPPTaskLog(const String & name, const MPPTaskId & mpp_task_id_)
{
    return std::make_shared<LogWithPrefix>(&Poco::Logger::get(name), mpp_task_id_.toString());
}

LogWithPrefixPtr getMPPTaskLog(const DAGContext & dag_context, const String & name)
{
    return getMPPTaskLog(dag_context.log, name, dag_context.getMPPTaskId());
}

LogWithPrefixPtr getMPPTaskLog(const LogWithPrefixPtr & log, const String & name, const MPPTaskId & mpp_task_id_ = MPPTaskId::unknown_mpp_task_id)
{
    if (log == nullptr)
    {
        return getMPPTaskLog(name, mpp_task_id_);
    }

    return log->append(name);
}
} // namespace DB
