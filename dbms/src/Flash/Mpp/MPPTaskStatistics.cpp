#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTaskStatistics.h>
#include <Flash/Mpp/getMPPTaskLog.h>
#include <fmt/format.h>
#include <tipb/executor.pb.h>

namespace DB
{
MPPTaskStatistics::MPPTaskStatistics(const LogWithPrefixPtr & log_, const MPPTaskId & id_, String address_)
    : log(getMPPTaskLog(log_, "mpp_task_tracing"))
    , id(id_)
    , host(std::move(address_))
    , task_init_timestamp(Clock::now())
    , status(INITIALIZING)
{}

void MPPTaskStatistics::start()
{
    task_start_timestamp = Clock::now();
    status = RUNNING;
}

void MPPTaskStatistics::end(const TaskStatus & status_, StringRef error_message_)
{
    task_end_timestamp = Clock::now();
    status = status_;
    error_message.assign(error_message_.data, error_message_.size);
    if (executor_statistics_collector.isInitialized())
    {
        executor_statistics_collector.collectRuntimeDetails();
    }
}

void MPPTaskStatistics::logStats()
{
    log->debug(toJson());
}

namespace
{
Int64 toNanoseconds(MPPTaskStatistics::Timestamp timestamp)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(timestamp.time_since_epoch()).count();
}
} // namespace

void MPPTaskStatistics::initializeExecutorDAG(DAGContext * dag_context)
{
    assert(dag_context);
    assert(dag_context->dag_request);
    assert(dag_context->isMPPTask());
    const auto & root_executor = dag_context->dag_request->root_executor();
    assert(root_executor.has_exchange_sender());
    sender_executor_id = root_executor.executor_id();
    executor_statistics_collector.initialize(dag_context);
}

String MPPTaskStatistics::toJson() const
{
    return fmt::format(
        R"({{"query_tso":{},"task_id":{},"sender_executor_id":"{}","executors":{},"host":"{}","task_init_timestamp":{},"compile_start_timestamp":{},"compile_end_timestamp":{},"task_start_timestamp":{},"task_end_timestamp":{},"status":"{}","error_message":"{}","working_time":{},"memory_peak":{}}})",
        id.start_ts,
        id.task_id,
        sender_executor_id,
        executor_statistics_collector.resToJson(),
        host,
        toNanoseconds(task_init_timestamp),
        toNanoseconds(compile_start_timestamp),
        toNanoseconds(compile_end_timestamp),
        toNanoseconds(task_start_timestamp),
        toNanoseconds(task_end_timestamp),
        taskStatusToString(status),
        error_message,
        working_time,
        memory_peak);
}
} // namespace DB