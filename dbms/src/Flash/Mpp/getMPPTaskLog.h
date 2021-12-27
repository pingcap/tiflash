#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <fmt/core.h>

namespace DB
{
LogWithPrefixPtr getMPPTaskLog(const String & name, const MPPTaskId & mpp_task_id_);

LogWithPrefixPtr getMPPTaskLog(const DAGContext & dag_context, const String & name);

LogWithPrefixPtr getMPPTaskLog(const LogWithPrefixPtr & log, const String & name, const MPPTaskId & mpp_task_id_ = MPPTaskId::unknown_mpp_task_id);

} // namespace DB
