// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/FmtUtils.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTaskStatistics.h>
#include <Flash/Mpp/getMPPTaskTracingLog.h>
#include <common/logger_useful.h>
#include <fmt/format.h>
#include <tipb/executor.pb.h>

#include <magic_enum.hpp>

namespace DB
{
MPPTaskStatistics::MPPTaskStatistics(const MPPTaskId & id_, String address_)
    : log(getMPPTaskTracingLog(id_))
    , executor_statistics_collector(log->identifier())
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
}

void MPPTaskStatistics::recordReadWaitIndex(DAGContext & dag_context)
{
    if (dag_context.has_read_wait_index)
    {
        read_wait_index_start_timestamp = dag_context.read_wait_index_start_timestamp;
        read_wait_index_end_timestamp = dag_context.read_wait_index_end_timestamp;
    }
    // else keep zero timestamp
}
namespace
{
Int64 toNanoseconds(MPPTaskStatistics::Timestamp timestamp)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(timestamp.time_since_epoch()).count();
}
} // namespace

void MPPTaskStatistics::initializeExecutorDAG(DAGContext * dag_context_)
{
    assert(dag_context_);
    assert(dag_context_->isMPPTask());
    dag_context = dag_context_;
    const auto & root_executor = dag_context->dag_request.rootExecutor();
    if unlikely (!root_executor.has_exchange_sender())
        throw TiFlashException(
            "The root executor isn't ExchangeSender in MPP, which is unexpected.",
            Errors::Coprocessor::BadRequest);

    is_root = dag_context->isRootMPPTask();
    sender_executor_id = root_executor.executor_id();
    executor_statistics_collector.initialize(dag_context);
}

void MPPTaskStatistics::collectRuntimeStatistics()
{
    const auto & executor_statistics_res = executor_statistics_collector.getProfiles();
    auto it = executor_statistics_res.find(sender_executor_id);
    RUNTIME_CHECK_MSG(
        it != executor_statistics_res.end(),
        "Can't find exchange sender statistics after `collectRuntimeStatistics`");
    const auto & return_statistics = it->second->getBaseRuntimeStatistics();
    // record io bytes
    output_bytes = return_statistics.bytes;
    recordInputBytes(*dag_context);
}

tipb::SelectResponse MPPTaskStatistics::genExecutionSummaryResponse()
{
    return executor_statistics_collector.genExecutionSummaryResponse();
}

tipb::TiFlashExecutionInfo MPPTaskStatistics::genTiFlashExecutionInfo()
{
    return executor_statistics_collector.genTiFlashExecutionInfo();
}

void MPPTaskStatistics::logTracingJson()
{
    LOG_IMPL(
        log,
        /// don't use info log for initializing status since it does not contains too many information
        status == INITIALIZING ? Poco::Message::PRIO_DEBUG : Poco::Message::PRIO_INFORMATION,
        R"({{"query_tso":{},"task_id":{},"is_root":{},"sender_executor_id":"{}","executors":{},"host":"{}")"
        R"(,"task_init_timestamp":{},"task_start_timestamp":{},"task_end_timestamp":{})"
        R"(,"compile_start_timestamp":{},"compile_end_timestamp":{})"
        R"(,"read_wait_index_start_timestamp":{},"read_wait_index_end_timestamp":{})"
        R"(,"local_input_bytes":{},"remote_input_bytes":{},"output_bytes":{})"
        R"(,"status":"{}","error_message":"{}","cpu_ru":{},"read_ru":{},"memory_peak":{},"extra_info":{}}})",
        id.gather_id.query_id.start_ts,
        id.task_id,
        is_root,
        sender_executor_id,
        executor_statistics_collector.profilesToJson(),
        host,
        toNanoseconds(task_init_timestamp),
        toNanoseconds(task_start_timestamp),
        toNanoseconds(task_end_timestamp),
        toNanoseconds(compile_start_timestamp),
        toNanoseconds(compile_end_timestamp),
        toNanoseconds(read_wait_index_start_timestamp),
        toNanoseconds(read_wait_index_end_timestamp),
        local_input_bytes,
        remote_input_bytes,
        output_bytes,
        magic_enum::enum_name(status),
        error_message,
        ru_info.cpu_ru,
        ru_info.read_ru,
        memory_peak,
        extra_info);
}

void MPPTaskStatistics::setMemoryPeak(Int64 memory_peak_)
{
    memory_peak = memory_peak_;
}

void MPPTaskStatistics::setRUInfo(const RUConsumption & ru_info_)
{
    ru_info = ru_info_;
    executor_statistics_collector.setLocalRUConsumption(ru_info_);
}

void MPPTaskStatistics::setCompileTimestamp(const Timestamp & start_timestamp, const Timestamp & end_timestamp)
{
    compile_start_timestamp = start_timestamp;
    compile_end_timestamp = end_timestamp;
}

void MPPTaskStatistics::setExtraInfo(const String & extra_info_)
{
    extra_info = extra_info_;
}

void MPPTaskStatistics::recordInputBytes(DAGContext & dag_context)
{
    switch (dag_context.getExecutionMode())
    {
    case ExecutionMode::None:
        break;
    case ExecutionMode::Stream:
        for (const auto & map_entry : dag_context.getInBoundIOInputStreamsMap())
        {
            for (const auto & io_stream : map_entry.second)
            {
                if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(io_stream.get()); p_stream)
                {
                    const auto & profile_info = p_stream->getProfileInfo();
                    if (dynamic_cast<ExchangeReceiverInputStream *>(p_stream)
                        || dynamic_cast<CoprocessorBlockInputStream *>(p_stream))
                        remote_input_bytes += profile_info.bytes;
                    else
                        local_input_bytes += profile_info.bytes;
                }
            }
        }
        break;
    case ExecutionMode::Pipeline:
        for (const auto & map_entry : dag_context.getInboundIOProfileInfosMap())
        {
            for (const auto & profile_info : map_entry.second)
            {
                if (profile_info->is_local)
                    local_input_bytes += profile_info->operator_info->bytes;
                else
                    remote_input_bytes += profile_info->operator_info->bytes;
            }
        }
        break;
    }
}
} // namespace DB
