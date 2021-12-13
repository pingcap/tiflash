#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Mpp/TaskStatus.h>
#include <common/StringRef.h>
#include <common/types.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#include <chrono>
#include <map>

namespace DB
{
struct MPPTaskStatistics
{
    using Clock = std::chrono::system_clock;
    using Timestamp = Clock::time_point;

    MPPTaskStatistics(const LogWithPrefixPtr & log_, const MPPTaskId & id_, String address_);

    void start();

    void end(const TaskStatus & status_, StringRef error_message_ = "");

    String toJson() const;

    void logStats();

    const LogWithPrefixPtr log;

    /// common
    const MPPTaskId id;
    const String host;
    Timestamp task_init_timestamp;
    Timestamp compile_start_timestamp;
    Timestamp compile_end_timestamp;
    Timestamp task_start_timestamp;
    Timestamp task_end_timestamp;
    TaskStatus status;
    String error_message;

    /// resource
    Int64 working_time = 0;
    Int64 memory_peak = 0;
};

using MPPTaskStatisticsPtr = std::shared_ptr<MPPTaskStatistics>;
} // namespace DB