#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <fmt/core.h>

namespace DB
{
inline LogWithPrefixPtr newMPPTaskLog(const String & name, const MPPTaskId & mpp_task_id_)
{
    return std::make_shared<LogWithPrefix>(&Poco::Logger::get(name), mpp_task_id_.toString());
}

inline LogWithPrefixPtr getMPPTaskLog(const LogWithPrefixPtr & log, const String & name, const MPPTaskId & mpp_task_id_ = MPPTaskId::empty_mpp_task_id)
{
    if (log == nullptr)
    {
        return newMPPTaskLog(name, mpp_task_id_);
    }

    return log->append(name);
}

} // namespace DB
