#pragma once

#include <Common/LogWithPrefix.h>
#include <fmt/core.h>

namespace DB
{
inline LogWithPrefixPtr getMPPTaskLog(const LogWithPrefixPtr & log, const String & name, const MPPTaskId & mpp_task_id_ = MPPTaskId::unknown_mpp_task_id)
{
    if (log == nullptr)
    {
        String prefix = mpp_task_id_.isUnknown() ? "[task: N/A query: N/A] " : fmt::format("[task: {} query: N/A] ", mpp_task_id_.task_id);
        return std::make_shared<LogWithPrefix>(&Poco::Logger::get(name), prefix);
    }

    return log->append(name);
}

} // namespace DB
