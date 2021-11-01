#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Mpp/TaskStatus.h>
#include <common/StringRef.h>
#include <common/types.h>

#include <chrono>
#include <mutex>

namespace DB
{
class MPPTaskStats
{
public:
    using Clock = std::chrono::system_clock;
    using Timestamp = Clock::time_point;
    using Duration = Int64; /// ns

    MPPTaskStats(const LogWithPrefixPtr & log_, const MPPTaskId & id_)
        : log(log_), id(id_), task_init_timestamp(Clock::now()), status(INITIALIZING)
    {}

    void start();

    void end(const TaskStatus & status_, StringRef error_message_ = "");

    String toString() const;

    const LogWithPrefixPtr log;

    /// common
    MPPTaskId id;
    String signature;
    String executor_structure;
    String inputstream_structure;
    Timestamp task_init_timestamp;
    Timestamp tunnels_init_start_timestamp;
    Timestamp tunnels_init_end_timestamp;
    Timestamp task_start_timestamp;
    Timestamp task_end_timestamp;
    Duration compile_duration = 0;
    Duration wait_index_duration = 0;
    TaskStatus status;
    String error_message;

    /// resource
    Int64 cpu_usage = 0;
    Int64 memory_peak = 0;
};
} // namespace DB