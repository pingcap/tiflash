#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Mpp/MPPTaskId.h>

namespace DB
{
inline constexpr auto tracing_log_source = "mpp_task_tracing";

LogWithPrefixPtr getMPPTaskTracingLog(const MPPTaskId & mpp_task_id);
} // namespace DB