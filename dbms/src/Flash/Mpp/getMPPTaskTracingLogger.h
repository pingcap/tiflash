#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Mpp/MPPTaskId.h>

namespace DB
{
constexpr auto tracing_log_source = "mpp_task_tracing";

LogWithPrefixPtr getMPPTaskTracingLogger(const MPPTaskId & mpp_task_id);
} // namespace DB