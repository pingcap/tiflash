#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Mpp/MPPTaskId.h>

namespace DB
{
class MPPTaskTracingLogger
{
public:
    static constexpr auto tracing_log_source = "mpp_task_tracing";

    explicit MPPTaskTracingLogger(const MPPTaskId & mpp_task_id);

    void log(const std::string & msg);

private:
    LogWithPrefixPtr logger;
};
} // namespace DB