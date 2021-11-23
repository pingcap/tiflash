#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Mpp/TaskStatus.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/StringRef.h>
#include <common/types.h>
#include <tipb/select.pb.h>

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

    MPPTaskStats(const LogWithPrefixPtr & log_, const MPPTaskId & id_, String address_);

    void start();

    void end(const TaskStatus & status_, StringRef error_message_ = "");

    String toJson() const;

    void setExecutorsStructure(const tipb::DAGRequest & dag_request);

    void setWaitIndexTimestamp(const Timestamp & wait_index_start_timestamp_, const Timestamp & wait_index_end_timestamp_);

    const LogWithPrefixPtr log;

    /// common
    const MPPTaskId id;
    const String host;
    std::vector<Int64> upstream_task_ids;
    String executors_structure;
    std::vector<ExecutorStatisticsPtr> executor_statistics_vec;
    Timestamp task_init_timestamp;
    Timestamp compile_start_timestamp;
    Timestamp wait_index_start_timestamp;
    Timestamp wait_index_end_timestamp;
    Timestamp compile_end_timestamp;
    Timestamp task_start_timestamp;
    Timestamp task_end_timestamp;
    TaskStatus status;
    String error_message;

    double local_input_throughput = 0.0;
    double remote_input_throughput = 0.0;
    double output_throughput = 0.0;

    /// resource
    Int64 cpu_usage = 0;
    Int64 memory_peak = 0;
};

using MPPTaskStatsPtr = std::shared_ptr<MPPTaskStats>;
} // namespace DB