#include <Common/FmtUtils.h>
#include <Common/joinStr.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTaskStats.h>
#include <Flash/Mpp/getMPPTaskLog.h>
#include <fmt/core.h>
#include <tipb/executor.pb.h>

namespace DB
{
MPPTaskStats::MPPTaskStats(const LogWithPrefixPtr & log_, const MPPTaskId & id_, String address_)
    : log(getMPPTaskLog(log_, "mpp_task_tracing"))
    , id(id_)
    , host(std::move(address_))
    , task_init_timestamp(Clock::now())
    , status(INITIALIZING)
{}

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
}

void MPPTaskStats::logStats()
{
    log->debug(toJson());
}

namespace
{
Int64 toNanoseconds(MPPTaskStats::Timestamp timestamp)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(timestamp.time_since_epoch()).count();
}

Int64 parseId(const String & executor_id)
{
    auto split_index = executor_id.find('_');
    if (split_index == String::npos || split_index == (executor_id.size() - 1))
    {
        throw TiFlashException("Illegal executor_id: " + executor_id, Errors::Coprocessor::Internal);
    }
    return std::stoi(executor_id.substr(split_index + 1, executor_id.size()));
}

String executorsToJson(std::map<String, ExecutorStatisticsPtr> * executor_statistics_map)
{
    assert(executor_statistics_map != nullptr);
    FmtBuffer buffer;
    buffer.append("[");
    joinStr(
        executor_statistics_map->cbegin(),
        executor_statistics_map->cend(),
        buffer,
        [](const auto & s, FmtBuffer & fb) {
            fb.append(s.second->toJson());
        },
        ",");
    buffer.append("]");
    return buffer.toString();
}
} // namespace

void MPPTaskStats::setSenderExecutorId(DAGContext & dag_context)
{
    assert(!dag_context.root_executor_id.empty());
    assert(dag_context.is_mpp_task);
    assert(dag_context.getExecutor(dag_context.root_executor_id)->tp() == tipb::ExecType::TypeExchangeSender);
    sender_executor_id = parseId(dag_context.root_executor_id);
}

String MPPTaskStats::toJson() const
{
    return fmt::format(
        R"({{"query_tso":{},"task_id":{},"sender_executor_id":{},"executors":{},"host":"{}","task_init_timestamp":{},"compile_start_timestamp":{},"wait_index_start_timestamp":{},"wait_index_end_timestamp":{},"compile_end_timestamp":{},"task_start_timestamp":{},"task_end_timestamp":{},"status":"{}","error_message":"{}","local_input_bytes":{},"remote_input_bytes":{},"output_bytes":{},"working_time":{},"memory_peak":{}}})",
        id.start_ts,
        id.task_id,
        sender_executor_id,
        executorsToJson(executor_statistics_map),
        host,
        toNanoseconds(task_init_timestamp),
        toNanoseconds(compile_start_timestamp),
        toNanoseconds(wait_index_start_timestamp),
        toNanoseconds(wait_index_end_timestamp),
        toNanoseconds(compile_end_timestamp),
        toNanoseconds(task_start_timestamp),
        toNanoseconds(task_end_timestamp),
        taskStatusToString(status),
        error_message,
        local_input_bytes,
        remote_input_bytes,
        output_bytes,
        working_time,
        memory_peak);
}

void MPPTaskStats::setWaitIndexTimestamp(const Timestamp & wait_index_start_timestamp_, const Timestamp & wait_index_end_timestamp_)
{
    if (toNanoseconds(wait_index_start_timestamp_) == 0 && toNanoseconds(wait_index_end_timestamp_) == 0)
    {
        wait_index_start_timestamp = compile_start_timestamp;
        wait_index_end_timestamp = compile_start_timestamp;
    }
    else
    {
        wait_index_start_timestamp = wait_index_start_timestamp_;
        wait_index_end_timestamp = wait_index_end_timestamp_;
    }
}
} // namespace DB