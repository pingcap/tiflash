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

    explicit MPPTaskStats(const LogWithPrefixPtr & log_)
        : log(log_)
    {}

    void init(const MPPTaskId & id_);

    void start();

    void end(const TaskStatus & status_, StringRef error_message_ = "");

    String toString() const;

    std::mutex mtx;
    const LogWithPrefixPtr log;

    /// common
    MPPTaskId id;
    String signature;
    String executor_structure;
    String inputstream_structure;
    Timestamp init_timestamp;
    Timestamp start_timestamp;
    Timestamp end_timestamp;
    Duration register_mpp_tunnel_duration = 0;
    Duration compile_duration = 0;
    Duration wait_index_duration = 0;
    Duration init_exchange_receiver_duration = 0;
    TaskStatus status;
    String error_message;

    /// resource
    Int64 cpu_usage = 0;
    Int64 memory_peak = 0;
};
} // namespace DB