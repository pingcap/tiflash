#pragma once

#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Mpp/TaskStatus.h>
#include <common/types.h>

#include <chrono>

namespace DB
{
class MPPTaskStats
{
public:
    using Timestamp = std::chrono::system_clock::time_point;
    using Duration = Int64;

    MPPTaskStats(MPPTaskId id_): id(id_)
    {}

private:
    /// common
    MPPTaskId id;
    String signature;
    String executor_structure;
    String inputstream_structure;
    Timestamp start_timestamp;
    Timestamp end_timestamp;
    Duration compile_duration = -1;
    Duration wait_index_duration = -1;
    TaskStatus status = INITIALIZING;
    String error_message;
};
}