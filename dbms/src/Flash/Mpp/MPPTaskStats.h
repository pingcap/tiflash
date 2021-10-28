#pragma once

#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Mpp/TaskStatus.h>
#include <common/StringRef.h>
#include <common/types.h>

#include <chrono>
#include <ctime>
#include <mutex>

namespace DB
{
class MPPTaskStats
{
public:
    using Clock = std::chrono::system_clock;
    using Timestamp = Clock::time_point;
    using Duration = Int64; /// ns

    void init(const MPPTaskId & id_)
    {
        std::lock_guard<std::mutex> lk(mtx);
        id = id_;
        init_timestamp = Clock::now();
        status = INITIALIZING;
    }

    void start()
    {
        std::lock_guard<std::mutex> lk(mtx);
        start_timestamp = Clock::now();
        status = RUNNING;
    }

    void end(const TaskStatus & status_, StringRef error_message_ = "")
    {
        std::lock_guard<std::mutex> lk(mtx);
        end_timestamp = Clock::now();
        status = status_;
        error_message.assign(error_message_.data, error_message_.size);
    }

    String toString() const
    {
        return fmt::format(
            "id: {}, init_timestamp: {}, start_timestamp: {}, end_timestamp: {}, compile_duration: {}, wait_index_duration: {}, status: {}, error_message: {}",
            id.toString(),
            init_timestamp.time_since_epoch().count(),
            start_timestamp.time_since_epoch().count(),
            end_timestamp.time_since_epoch().count(),
            compile_duration,
            wait_index_duration,
            taskStatusToString(status),
            error_message);
    }

    std::mutex mtx;

    /// common
    MPPTaskId id;
    String signature;
    String executor_structure;
    String inputstream_structure;
    Timestamp init_timestamp;
    Timestamp start_timestamp;
    Timestamp end_timestamp;
    Duration compile_duration = -1;
    Duration wait_index_duration = 0;
    TaskStatus status;
    String error_message;
};
} // namespace DB