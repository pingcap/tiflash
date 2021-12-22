#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Mpp/MPPTaskId.h>

namespace DB
{
/// Tracing logs are filtered by SourceFilterChannel.
inline constexpr auto tracing_log_source = "mpp_task_tracing";

/// All tracing logs must logged by the logger that got by `getMPPTaskTracingLog`.
LogWithPrefixPtr getMPPTaskTracingLog(const MPPTaskId & mpp_task_id);
} // namespace DB