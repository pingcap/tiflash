#pragma once

#include <Common/LogWithPrefix.h>
#include <fmt/core.h>

namespace DB
{
inline LogWithPrefixPtr getMPPTaskLog(const LogWithPrefixPtr & log, const String & name, Int64 mpp_task_id_ = -1)
{
    if (log == nullptr)
    {
        String prefix = mpp_task_id_ == -1 ? "[task: N/A query: N/A] " : fmt::format("[task: {} query: N/A] ", mpp_task_id_);
        return std::make_shared<LogWithPrefix>(&Poco::Logger::get(name), prefix);
    }

    return log->append(name);
}

} // namespace DB
