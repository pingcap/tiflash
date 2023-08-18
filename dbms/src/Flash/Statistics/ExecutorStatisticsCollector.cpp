<<<<<<< HEAD
// Copyright 2022 PingCAP, Ltd.
=======
// Copyright 2023 PingCAP, Inc.
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
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
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/CommonExecutorImpl.h>
#include <Flash/Statistics/ExchangeReceiverImpl.h>
#include <Flash/Statistics/ExchangeSenderImpl.h>
#include <Flash/Statistics/ExecutorStatisticsCollector.h>
#include <Flash/Statistics/JoinImpl.h>
#include <Flash/Statistics/TableScanImpl.h>
#include <Flash/Statistics/traverseExecutors.h>

namespace DB
{
DAGContext & ExecutorStatisticsCollector::getDAGContext() const
{
<<<<<<< HEAD
    assert(dag_context);
    return *dag_context;
=======
RemoteExecutionSummary getRemoteExecutionSummariesFromExchange(DAGContext & dag_context)
{
    RemoteExecutionSummary exchange_execution_summary;
    switch (dag_context.getExecutionMode())
    {
    case ExecutionMode::None:
        break;
    case ExecutionMode::Stream:
        for (const auto & map_entry : dag_context.getInBoundIOInputStreamsMap())
        {
            for (const auto & stream_ptr : map_entry.second)
            {
                if (auto * exchange_receiver_stream_ptr = dynamic_cast<ExchangeReceiverInputStream *>(stream_ptr.get());
                    exchange_receiver_stream_ptr)
                    exchange_execution_summary.merge(exchange_receiver_stream_ptr->getRemoteExecutionSummary());
            }
        }
        break;
    case ExecutionMode::Pipeline:
        for (const auto & map_entry : dag_context.getInboundIOProfileInfosMap())
        {
            for (const auto & profile_info : map_entry.second)
            {
                if (!profile_info->is_local)
                    exchange_execution_summary.merge(profile_info->remote_execution_summary);
            }
        }
        break;
    }
    return exchange_execution_summary;
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
}

String ExecutorStatisticsCollector::resToJson() const
{
    FmtBuffer buffer;
    buffer.append("[");
    buffer.joinStr(
<<<<<<< HEAD
        res.cbegin(),
        res.cend(),
        [](const auto & s, FmtBuffer & fb) {
            fb.append(s.second->toJson());
        },
=======
        profiles.cbegin(),
        profiles.cend(),
        [](const auto & s, FmtBuffer & fb) { fb.append(s.second->toJson()); },
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
        ",");
    buffer.append("]");
    return buffer.toString();
}

void ExecutorStatisticsCollector::initialize(DAGContext * dag_context_)
{
    assert(dag_context_);
    dag_context = dag_context_;
    assert(dag_context->dag_request);
    traverseExecutors(dag_context->dag_request, [&](const tipb::Executor & executor) {
        assert(executor.has_executor_id());
        const auto & executor_id = executor.executor_id();
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
                WindowStatistics>(executor_id, &executor))
        {
            throw TiFlashException(
                fmt::format("Unknown executor type, executor_id: {}", executor_id),
                Errors::Coprocessor::Internal);
        }
        return true;
    });
<<<<<<< HEAD
=======

    fillChildren();
}

void ExecutorStatisticsCollector::fillChildren()
{
    if (dag_context->dag_request.isTreeBased())
    {
        // set children for tree-based executors
        dag_context->dag_request.traverse([&](const tipb::Executor & executor) {
            std::vector<String> children;
            getChildren(executor).forEach([&](const tipb::Executor & child) {
                assert(child.has_executor_id());
                children.push_back(child.executor_id());
            });
            profiles[executor.executor_id()]->setChildren(children);
            return true;
        });
    }
    else
    {
        // fill list-based executors child
        std::optional<String> child;
        for (const auto & executor_id : dag_context->dag_request.list_based_executors_order)
        {
            if (child)
                profiles[executor_id]->setChild(*child);
            child = executor_id;
        }
    }
}

tipb::SelectResponse ExecutorStatisticsCollector::genExecutionSummaryResponse()
{
    tipb::SelectResponse response;
    fillExecuteSummaries(response);
    return response;
}

tipb::TiFlashExecutionInfo ExecutorStatisticsCollector::genTiFlashExecutionInfo()
{
    tipb::SelectResponse response = genExecutionSummaryResponse();
    tipb::TiFlashExecutionInfo execution_info;
    auto * execution_summaries = execution_info.mutable_execution_summaries();
    execution_summaries->CopyFrom(response.execution_summaries());
    return execution_info;
}

void ExecutorStatisticsCollector::fillExecutionSummary(
    tipb::SelectResponse & response,
    const String & executor_id,
    const BaseRuntimeStatistics & statistic,
    UInt64 join_build_time,
    const std::unordered_map<String, DM::ScanContextPtr> & scan_context_map) const
{
    ExecutionSummary current;
    current.fill(statistic);
    current.time_processed_ns += join_build_time;
    // merge detailed table scan profile
    if (const auto & iter = scan_context_map.find(executor_id); iter != scan_context_map.end())
        current.scan_context->merge(*(iter->second));

    current.time_processed_ns += dag_context->compile_time_ns;
    fillTiExecutionSummary(
        *dag_context,
        response.add_execution_summaries(),
        current,
        executor_id,
        force_fill_executor_id);
}

void ExecutorStatisticsCollector::fillExecuteSummaries(tipb::SelectResponse & response)
{
    if (!dag_context->collect_execution_summaries)
        return;

    collectRuntimeDetails();

    fillLocalExecutionSummaries(response);

    // TODO: remove filling remote execution summaries
    fillRemoteExecutionSummaries(response);
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
}

void ExecutorStatisticsCollector::collectRuntimeDetails()
{
    assert(dag_context);
    for (const auto & entry : res)
    {
        entry.second->collectRuntimeDetail();
    }
}
<<<<<<< HEAD
} // namespace DB
=======

void ExecutorStatisticsCollector::fillRemoteExecutionSummaries(tipb::SelectResponse & response)
{
    // TODO: support cop remote read and disaggregated mode.
    auto exchange_execution_summary = getRemoteExecutionSummariesFromExchange(*dag_context);

    // fill execution_summaries from remote executor received by exchange.
    for (auto & p : exchange_execution_summary.execution_summaries)
        fillTiExecutionSummary(
            *dag_context,
            response.add_execution_summaries(),
            p.second,
            p.first,
            force_fill_executor_id);
}

} // namespace DB
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
