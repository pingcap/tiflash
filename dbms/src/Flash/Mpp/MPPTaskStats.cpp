#include <Flash/Mpp/MPPTaskStats.h>
#include <fmt/core.h>
#include <fmt/format.h>

namespace DB
{
void MPPTaskStats::start()
{
    task_start_timestamp = Clock::now();
    status = RUNNING;
}

void MPPTaskStats::end(const TaskStatus & status_, StringRef error_message_)
{
    task_end_timestamp = Clock::now();
    status = status_;
    error_message.assign(error_message_.data, error_message_.size);
    log->debug(toString());
}

String MPPTaskStats::toString() const
{
    return fmt::format(
        R"(task_tracing: {{"query_tso":{},"task_id":{},"host":"{}","upstream_task_ids":[{}],"task_init_timestamp":{},"compile_start_timestamp":{},"wait_index_start_timestamp":{},"wait_index_end_timestamp":{},"compile_end_timestamp":{},"task_start_timestamp":{},"task_end_timestamp":{},"status":"{}","error_message":"{}","local_input_throughput":{},"remote_input_throughput":{},"output_throughput":{},"cpu_usage":{},"memory_peak":{}}})",
        id.start_ts,
        id.task_id,
        host,
        fmt::join(upstream_task_ids, ","),
        Clock::to_time_t(task_init_timestamp),
        Clock::to_time_t(compile_start_timestamp),
        Clock::to_time_t(wait_index_start_timestamp),
        Clock::to_time_t(wait_index_end_timestamp),
        Clock::to_time_t(compile_end_timestamp),
        Clock::to_time_t(task_start_timestamp),
        Clock::to_time_t(task_end_timestamp),
        taskStatusToString(status),
        error_message,
        local_input_throughput,
        remote_input_throughput,
        output_throughput,
        cpu_usage,
        memory_peak);
}
} // namespace DB