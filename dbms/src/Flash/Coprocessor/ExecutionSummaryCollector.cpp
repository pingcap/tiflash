// Copyright 2022 PingCAP, Ltd.
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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/ExecutionSummaryCollector.h>
#include <Flash/Coprocessor/RemoteExecutionSummary.h>
#include <Flash/Statistics/traverseExecutors.h>

namespace DB
{
namespace
{
RemoteExecutionSummary getRemoteExecutionSummariesFromExchange(DAGContext & dag_context)
{
    RemoteExecutionSummary exchange_execution_summary;
    for (const auto & map_entry : dag_context.getInBoundIOInputStreamsMap())
    {
        for (const auto & stream_ptr : map_entry.second)
        {
            if (auto * exchange_receiver_stream_ptr = dynamic_cast<ExchangeReceiverInputStream *>(stream_ptr.get()); exchange_receiver_stream_ptr)
            {
                exchange_execution_summary.merge(exchange_receiver_stream_ptr->getRemoteExecutionSummary());
            }
        }
    }
    return exchange_execution_summary;
}
} // namespace

void ExecutionSummaryCollector::fillTiExecutionSummary(
    tipb::ExecutorExecutionSummary * execution_summary,
    ExecutionSummary & current,
    const String & executor_id) const
{
    execution_summary->set_time_processed_ns(current.time_processed_ns);
    execution_summary->set_num_produced_rows(current.num_produced_rows);
    execution_summary->set_num_iterations(current.num_iterations);
    execution_summary->set_concurrency(current.concurrency);
    execution_summary->mutable_tiflash_scan_context()->CopyFrom(current.scan_context->serialize());

    if (dag_context.return_executor_id)
        execution_summary->set_executor_id(executor_id);
}

tipb::SelectResponse ExecutionSummaryCollector::genExecutionSummaryResponse()
{
    tipb::SelectResponse response;
    if (enable_pipeline)
        addExecuteSummariesForPipeline(response);
    else
        addExecuteSummaries(response);
    return response;
}

tipb::SelectResponse ExecutionSummaryCollector::genExecutionSummaryResponseForPipeline()
{
    tipb::SelectResponse response;
    addExecuteSummariesForPipeline(response);
    return response;
}

void ExecutionSummaryCollector::fillLocalExecutionSummary(
    tipb::SelectResponse & response,
    const String & executor_id,
    const BlockInputStreams & streams,
    const std::unordered_map<String, DM::ScanContextPtr> & scan_context_map) const
{
    ExecutionSummary current;
    /// part 1: local execution info
    // get execution info from streams
    for (const auto & stream_ptr : streams)
    {
        if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream_ptr.get()))
        {
            current.time_processed_ns = std::max(current.time_processed_ns, p_stream->getProfileInfo().execution_time);
            current.num_produced_rows += p_stream->getProfileInfo().rows;
            current.num_iterations += p_stream->getProfileInfo().blocks;
        }
        ++current.concurrency;
    }
    // get execution info from scan_context
    if (const auto & iter = scan_context_map.find(executor_id); iter != scan_context_map.end())
    {
        current.scan_context->merge(*(iter->second));
    }
    /// part 2: for join need to add the build time
    /// In TiFlash, a hash join's build side is finished before probe side starts,
    /// so the join probe side's running time does not include hash table's build time,
    /// when construct ExecSummaries, we need add the build cost to probe executor
    auto all_join_id_it = dag_context.getExecutorIdToJoinIdMap().find(executor_id);
    if (all_join_id_it != dag_context.getExecutorIdToJoinIdMap().end())
    {
        for (const auto & join_executor_id : all_join_id_it->second)
        {
            auto it = dag_context.getJoinExecuteInfoMap().find(join_executor_id);
            if (it != dag_context.getJoinExecuteInfoMap().end())
            {
                UInt64 process_time_for_build = 0;
                for (const auto & join_build_stream : it->second.join_build_streams)
                {
                    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(join_build_stream.get()); p_stream)
                        process_time_for_build = std::max(process_time_for_build, p_stream->getProfileInfo().execution_time);
                }
                current.time_processed_ns += process_time_for_build;
            }
        }
    }

    current.time_processed_ns += dag_context.compile_time_ns;
    fillTiExecutionSummary(response.add_execution_summaries(), current, executor_id);
}

void ExecutionSummaryCollector::addExecuteSummaries(tipb::SelectResponse & response)
{
    if (!dag_context.collect_execution_summaries)
        return;

    /// fill execution_summary for local executor
    if (dag_context.return_executor_id)
    {
        for (auto & p : dag_context.getProfileStreamsMap())
            fillLocalExecutionSummary(response, p.first, p.second, dag_context.scan_context_map);
    }
    else
    {
        const auto & profile_streams_map = dag_context.getProfileStreamsMap();
        assert(profile_streams_map.size() == dag_context.list_based_executors_order.size());
        for (const auto & executor_id : dag_context.list_based_executors_order)
        {
            auto it = profile_streams_map.find(executor_id);
            assert(it != profile_streams_map.end());
            fillLocalExecutionSummary(response, executor_id, it->second, dag_context.scan_context_map);
        }
    }

    // TODO support cop remote read and disaggregated mode.
    auto exchange_execution_summary = getRemoteExecutionSummariesFromExchange(dag_context);
    // fill execution_summary to reponse for remote executor received by exchange.
    for (auto & p : exchange_execution_summary.execution_summaries)
    {
        fillTiExecutionSummary(response.add_execution_summaries(), p.second, p.first);
    }
}

void ExecutionSummaryCollector::addExecuteSummariesForPipeline(tipb::SelectResponse & response)
{
    if (!dag_context.collect_execution_summaries)
        return;
    UInt64 accumulated_time = 0;
    /// fill execution_summary for local executor
    if (dag_context.return_executor_id)
    {
        assert(dag_context.dag_request->has_root_executor());
        UInt64 accumulated_time = 0;

        traverseExecutorsReverse(dag_context.dag_request, [&](const tipb::Executor & executor) {
            auto && executor_id = executor.executor_id();
            accumulated_time = fillLocalExecutionSummaryForPipeline(
                response,
                executor_id,
                dag_context.pipeline_profiles[executor_id],
                accumulated_time,
                dag_context.scan_context_map);
        });
    }
    else
    {
        /// collect list based executor's excution summary
        const auto & profile_map = dag_context.pipeline_profiles;
        assert(profile_map.size() == dag_context.list_based_executors_order.size());
        for (const auto & executor_id : dag_context.list_based_executors_order)
        {
            auto it = profile_map.find(executor_id);
            assert(it != profile_map.end());
            accumulated_time = fillLocalExecutionSummaryForPipeline(
                response,
                executor_id,
                it->second,
                accumulated_time,
                dag_context.scan_context_map);
        }
    }

    // TODO: consider cop remote read and disaggregated mode.
}

UInt64 ExecutionSummaryCollector::fillLocalExecutionSummaryForPipeline(
    tipb::SelectResponse & response,
    const String & executor_id,
    const ExecutorProfileInfo & executor_profile,
    UInt64 accumulated_time,
    const std::unordered_map<String, DM::ScanContextPtr> & scan_context_map) const
{
    ExecutionSummary current;
    for (size_t i = 0; i < executor_profile.size(); ++i)
    {
        auto && operator_profile_group = executor_profile[i];
        auto concurrency = operator_profile_group.size();
        if (i == executor_profile.size() - 1)
        {
            ExecutionSummary current_operator;
            UInt64 time_processed_ns = 0;
            current.concurrency = concurrency;
            for (const auto & operator_profile : operator_profile_group)
            {
                time_processed_ns = std::max(time_processed_ns, operator_profile->execution_time);
                current.num_produced_rows += operator_profile->rows;
                current.num_iterations += operator_profile->blocks;
            }
            accumulated_time += time_processed_ns;
            current.time_processed_ns = accumulated_time;
        }
        else
        {
            ExecutionSummary current_operator;
            current_operator.concurrency = concurrency;
            for (const auto & operator_profile : operator_profile_group)
            {
                current_operator.time_processed_ns = std::max(current.time_processed_ns, operator_profile->execution_time);
            }
            accumulated_time += current_operator.time_processed_ns;
        }
    }
    // get execution info from scan_context
    if (const auto & iter = scan_context_map.find(executor_id); iter != scan_context_map.end())
    {
        current.scan_context->merge(*(iter->second));
    }
    fillTiExecutionSummary(response.add_execution_summaries(), current, executor_id);

    return accumulated_time;
}
} // namespace DB
