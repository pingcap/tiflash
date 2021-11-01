#include <Flash/Mpp/MPPTaskStats.h>
#include <fmt/core.h>

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
        "MPPTaskStats: {{id: {}, task_init_timestamp: {}, tunnels_init_start_timestamp: {}, tunnels_init_end_timestamp: {}, task_start_timestamp: {}, task_end_timestamp: {}, compile_duration: {} ns, wait_index_duration: {} ns, status: {}, error_message: {}, cpu_usage: {}, memory_peak: {}}}",
        id.toString(),
        Clock::to_time_t(task_init_timestamp),
        Clock::to_time_t(tunnels_init_start_timestamp),
        Clock::to_time_t(tunnels_init_end_timestamp),
        Clock::to_time_t(task_start_timestamp),
        Clock::to_time_t(task_end_timestamp),
        compile_duration,
        wait_index_duration,
        taskStatusToString(status),
        error_message,
        cpu_usage,
        memory_peak);
}
} // namespace DB