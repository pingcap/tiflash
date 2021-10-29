#include <Flash/Mpp/MPPTaskStats.h>
#include <fmt/core.h>

namespace DB
{
void MPPTaskStats::init(const MPPTaskId & id_)
{
    std::lock_guard<std::mutex> lk(mtx);
    id = id_;
    init_timestamp = Clock::now();
    status = INITIALIZING;
}

void MPPTaskStats::start()
{
    std::lock_guard<std::mutex> lk(mtx);
    start_timestamp = Clock::now();
    status = RUNNING;
}

void MPPTaskStats::end(const TaskStatus & status_, StringRef error_message_)
{
    std::lock_guard<std::mutex> lk(mtx);
    end_timestamp = Clock::now();
    status = status_;
    error_message.assign(error_message_.data, error_message_.size);
    log->debug(toString());
}

String MPPTaskStats::toString() const
{
    return fmt::format(
        "MPPTaskStats: {{id: {}, init_timestamp: {}, start_timestamp: {}, end_timestamp: {}, register_mpp_tunnel_duration: {} ns, compile_duration: {} ns, wait_index_duration: {} ns, init_exchange_receiver_duration: {} ns, status: {}, error_message: {}, memory_peak: {}}}",
        id.toString(),
        Clock::to_time_t(init_timestamp),
        Clock::to_time_t(start_timestamp),
        Clock::to_time_t(end_timestamp),
        register_mpp_tunnel_duration,
        compile_duration,
        wait_index_duration,
        init_exchange_receiver_duration,
        taskStatusToString(status),
        error_message,
        memory_peak);
}
} // namespace DB