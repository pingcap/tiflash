// Copyright 2023 PingCAP, Ltd.
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
#include <Flash/Coprocessor/RemoteExecutionSummary.h>
#include <Flash/Statistics/CommonExecutorImpl.h>
#include <Flash/Statistics/ExchangeReceiverImpl.h>
#include <Flash/Statistics/ExchangeSenderImpl.h>
#include <Flash/Statistics/ExecutionSummaryHelper.h>
#include <Flash/Statistics/ExecutorStatisticsCollector.h>
#include <Flash/Statistics/JoinImpl.h>
#include <Flash/Statistics/TableScanImpl.h>
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

String ExecutorStatisticsCollector::profilesToJson() const
{
    FmtBuffer buffer;
    buffer.append("[");
    buffer.joinStr(
        profiles.cbegin(),
        profiles.cend(),
        [](const auto & s, FmtBuffer & fb) {
            fb.append(s.second->toJson());
        },
        ",");
    buffer.append("]");
    return buffer.toString();
}

void ExecutorStatisticsCollector::initialize(DAGContext * dag_context_)
{
    dag_context = dag_context_;
    assert(dag_context);
    RUNTIME_CHECK(dag_context->dag_request);

    traverseExecutors(dag_context->dag_request, [&](const tipb::Executor & executor) {
        RUNTIME_CHECK(executor.has_executor_id());
        if (!append<
                AggStatistics,
                ExchangeReceiverStatistics,
                ExchangeSenderStatistics,
                FilterStatistics,
                JoinStatistics,
                LimitStatistics,
                ProjectStatistics,
                SortStatistics,
                TableScanStatistics,
                TopNStatistics,
                WindowStatistics,
                ExpandStatistics>(&executor))
        {
            throw TiFlashException(
                fmt::format("Unknown executor type, executor_id: {}", executor.executor_id()),
                Errors::Coprocessor::Internal);
        }
        return true;
    });

    fillListBasedExecutorsChild();
}

void ExecutorStatisticsCollector::fillListBasedExecutorsChild()
{
    if (!dag_context->return_executor_id)
    {
        // fill list-based executors child
        auto size = dag_context->dag_request->executors_size();
        RUNTIME_CHECK(size > 0);
        const auto & executors = dag_context->dag_request->executors();
        String child;
        for (int i = 0; i < size; ++i)
        {
            const auto & executor_id = executors[i].executor_id();
            if (i != 0)
                profiles[executor_id]->setChild(child);
            child = executor_id;
        }
    }
}

tipb::SelectResponse ExecutorStatisticsCollector::genExecutionSummaryResponse()
{
    tipb::SelectResponse response;
    addExecuteSummaries(response);
    return response;
}

void ExecutorStatisticsCollector::fillExecutionSummary(
    tipb::SelectResponse & response,
    const String & executor_id,
    const BaseRuntimeStatistics & statistic,
    UInt64 join_build_time,
    const std::unordered_map<String, DM::ScanContextPtr> & scan_context_map) const
{
    ExecutionSummary current;
    current.set(statistic);
    current.time_processed_ns += join_build_time;
    // merge detailed table scan profile
    if (const auto & iter = scan_context_map.find(executor_id); iter != scan_context_map.end())
        current.scan_context->merge(*(iter->second));

    current.time_processed_ns += dag_context->compile_time_ns;
    fillTiExecutionSummary(*dag_context, response.add_execution_summaries(), current, executor_id, fill_executor_id);
}

void ExecutorStatisticsCollector::addExecuteSummaries(tipb::SelectResponse & response)
{
    if (!dag_context->collect_execution_summaries)
        return;

    collectRuntimeDetails();

    addLocalExecutionSummaries(response);

    // TODO: remove filling remote execution summaries
    addRemoteExecutionSummaries(response);
}

void ExecutorStatisticsCollector::collectRuntimeDetails()
{
    assert(dag_context);
    for (const auto & entry : profiles)
        entry.second->collectRuntimeDetail();
}

void ExecutorStatisticsCollector::addLocalExecutionSummaries(tipb::SelectResponse & response)
{
    if (dag_context->return_executor_id)
    {
        // fill in tree-based executors' execution summary
        for (auto & p : profiles)
            fillExecutionSummary(
                response,
                p.first,
                p.second->getBaseRuntimeStatistics(),
                p.second->processTimeForJoinBuild(),
                dag_context->scan_context_map);
    }
    else
    {
        // fill in list-based executors' execution summary
        RUNTIME_CHECK(profiles.size() == dag_context->list_based_executors_order.size());
        for (const auto & executor_id : dag_context->list_based_executors_order)
        {
            auto it = profiles.find(executor_id);
            RUNTIME_CHECK(it != profiles.end());
            fillExecutionSummary(
                response,
                executor_id,
                it->second->getBaseRuntimeStatistics(),
                0, // No join executors in list-based executors
                dag_context->scan_context_map);
        }
    }
}

void ExecutorStatisticsCollector::addRemoteExecutionSummaries(tipb::SelectResponse & response)
{
    // TODO: support cop remote read and disaggregated mode.
    auto exchange_execution_summary = getRemoteExecutionSummariesFromExchange(*dag_context);

    // fill execution_summaries from remote executor received by exchange.
    for (auto & p : exchange_execution_summary.execution_summaries)
        fillTiExecutionSummary(*dag_context, response.add_execution_summaries(), p.second, p.first, fill_executor_id);
}

} // namespace DB
