#include <Common/TiFlashException.h>
#include <Flash/Mpp/MPPTaskStats.h>
#include <Flash/Mpp/getMPPTaskLog.h>
#include <fmt/core.h>
#include <fmt/format.h>
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
    log->debug(toString());
}

namespace
{
String parseId(const String & executor_id)
{
    auto split_index = executor_id.find('_');
    if (split_index == String::npos || split_index == (executor_id.size() - 1))
    {
        throw TiFlashException("Illegal executor_id: " + executor_id, Errors::Coprocessor::Internal);
    }
    return executor_id.substr(split_index + 1, executor_id.size());
}

String toJson(const tipb::Executor & executor);

String childrenToJson(const tipb::Executor & executor)
{
    switch (executor.tp())
    {
    case tipb::ExecType::TypeTableScan:
        break;
    case tipb::ExecType::TypeJoin:
        return fmt::format("{},{}", toJson(executor.join().children(0)), toJson(executor.join().children(1)));
    case tipb::ExecType::TypeIndexScan:
        throw TiFlashException("IndexScan is not supported", Errors::Coprocessor::Unimplemented);
    case tipb::ExecType::TypeSelection:
        return toJson(executor.selection().child());
    case tipb::ExecType::TypeAggregation:
    case tipb::ExecType::TypeStreamAgg:
        return toJson(executor.aggregation().child());
    case tipb::ExecType::TypeTopN:
        return toJson(executor.topn().child());
    case tipb::ExecType::TypeLimit:
        return toJson(executor.limit().child());
    case tipb::ExecType::TypeProjection:
        return toJson(executor.projection().child());
    case tipb::ExecType::TypeExchangeSender:
        return toJson(executor.exchange_sender().child());
    case tipb::ExecType::TypeExchangeReceiver:
        break;
    case tipb::ExecType::TypeKill:
        throw TiFlashException("Kill executor is not supported", Errors::Coprocessor::Unimplemented);
    default:
        throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
    }
    return "";
}

String toJson(const tipb::Executor & executor)
{
    if (!executor.has_executor_id())
        throw TiFlashException("Illegal mpp dag_request: `!executor.has_executor_id()`", Errors::Coprocessor::BadRequest);
    return fmt::format(R"({{"id":"{}","children":[{}]}})", parseId(executor.executor_id()), childrenToJson(executor));
}

Int64 toMicroseconds(MPPTaskStats::Timestamp timestamp)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timestamp.time_since_epoch()).count();
}
} // namespace

void MPPTaskStats::setExecutorsStructure(const tipb::DAGRequest & dag_request)
{
    if (!dag_request.has_root_executor())
        throw TiFlashException("Illegal mpp dag_request: `!dag_request.has_root_executor()`", Errors::Coprocessor::BadRequest);
    executors_structure = toJson(dag_request.root_executor());
}

String MPPTaskStats::toString() const
{
    return fmt::format(
        R"({{"query_tso":{},"task_id":{},"executors_structure":{},"host":"{}","upstream_task_ids":[{}],"task_init_timestamp":{},"compile_start_timestamp":{},"wait_index_start_timestamp":{},"wait_index_end_timestamp":{},"compile_end_timestamp":{},"task_start_timestamp":{},"task_end_timestamp":{},"status":"{}","error_message":"{}","local_input_throughput":{},"remote_input_throughput":{},"output_throughput":{},"cpu_usage":{},"memory_peak":{}}})",
        id.start_ts,
        id.task_id,
        executors_structure,
        host,
        fmt::join(upstream_task_ids, ","),
        toMicroseconds(task_init_timestamp),
        toMicroseconds(compile_start_timestamp),
        toMicroseconds(wait_index_start_timestamp),
        toMicroseconds(wait_index_end_timestamp),
        toMicroseconds(compile_end_timestamp),
        toMicroseconds(task_start_timestamp),
        toMicroseconds(task_end_timestamp),
        taskStatusToString(status),
        error_message,
        local_input_throughput,
        remote_input_throughput,
        output_throughput,
        cpu_usage,
        memory_peak);
}

void MPPTaskStats::setWaitIndexTimestamp(const Timestamp & wait_index_start_timestamp_, const Timestamp & wait_index_end_timestamp_)
{
    if (toMicroseconds(wait_index_start_timestamp_) == 0 && toMicroseconds(wait_index_end_timestamp_) == 0)
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