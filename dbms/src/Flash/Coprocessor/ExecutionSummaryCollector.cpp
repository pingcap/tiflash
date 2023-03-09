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
#include <Flash/Statistics/ExchangeReceiverImpl.h>

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

    if (dag_context.return_executor_id || fill_executor_id)
        execution_summary->set_executor_id(executor_id);
}

tipb::SelectResponse ExecutionSummaryCollector::genExecutionSummaryResponse()
{
    tipb::SelectResponse response;
    addExecuteSummaries(response);
    return response;
}

void ExecutionSummaryCollector::fillExecutionSummary(
    tipb::SelectResponse & response,
    const String & executor_id,
    const std::unordered_map<String, DM::ScanContextPtr> & scan_context_map)
{
    ExecutionSummary current;
    const auto & statistic_result = dag_context.executorStatisticCollector()->getResult();
    auto it = statistic_result.find(executor_id);
    RUNTIME_CHECK(it != statistic_result.end());

    const auto & statistic = it->second->getBaseRuntimeStatistics();

    current.num_iterations = statistic.blocks;
    current.num_produced_rows = statistic.rows;
    current.concurrency = statistic.concurrency;
    current.time_processed_ns = statistic.execution_time_ns;

    if (const auto & iter = scan_context_map.find(executor_id); iter != scan_context_map.end())
        current.scan_context->merge(*(iter->second));

    current.time_processed_ns += dag_context.compile_time_ns;
    fillTiExecutionSummary(response.add_execution_summaries(), current, executor_id);
}

void ExecutionSummaryCollector::collect()
{
    dag_context.executorStatisticCollector()->collectRuntimeDetails();
}

void ExecutionSummaryCollector::addExecuteSummaries(tipb::SelectResponse & response)
{
    if (!dag_context.collect_execution_summaries)
        return;
    LOG_DEBUG(log, "start collecting execution summary");

    collect();

    if (dag_context.return_executor_id)
    {
        auto profiles = dag_context.executorStatisticCollector()->getResult();
        for (auto & p : profiles)
            fillExecutionSummary(response, p.first, dag_context.scan_context_map);
    }
    else
    {
        auto profiles = dag_context.executorStatisticCollector()->getResult();
        assert(profiles.size() == dag_context.list_based_executors_order.size());
        for (const auto & executor_id : dag_context.list_based_executors_order)
        {
            auto it = profiles.find(executor_id);
            assert(it != profiles.end());
            fillExecutionSummary(response, executor_id, dag_context.scan_context_map);
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
} // namespace DB
