#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Mpp/TaskStatus.h>
#include <common/StringRef.h>
#include <common/types.h>

#include <chrono>
#include <mutex>
#include <vector>

namespace DB
{
struct MPPTaskStats
{
    using Clock = std::chrono::system_clock;
    using Timestamp = Clock::time_point;
    using Duration = Int64; /// ns

    MPPTaskStats(const LogWithPrefixPtr & log_, const MPPTaskId & id_, String address_)
        : log(log_)
        , id(id_)
        , node_host(std::move(address_))
        , task_init_timestamp(Clock::now())
        , status(INITIALIZING)
    {}

    void start();

    void end(const TaskStatus & status_, StringRef error_message_ = "");

    String toString() const;

    const LogWithPrefixPtr log;

    /// common
    const MPPTaskId id;
    const String node_host;
    std::vector<Int64> upstream_task_ids;
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

    double input_throughput = 0.0;
    double output_throughput = 0.0;

    /// resource
    Int64 cpu_usage = 0;
    Int64 memory_peak = 0;
};

using MPPTaskStatsPtr = std::shared_ptr<MPPTaskStats>;
} // namespace DB