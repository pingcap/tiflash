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
    assert(dag_context);
    return *dag_context;
}

String ExecutorStatisticsCollector::resToJson() const
{
    FmtBuffer buffer;
    buffer.append("[");
    buffer.joinStr(
        res.cbegin(),
        res.cend(),
        [](const auto & s, FmtBuffer & fb) {
            fb.append(s.second->toJson());
        },
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
        RUNTIME_CHECK(executor.has_executor_id());
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
}

void ExecutorStatisticsCollector::collectRuntimeDetails()
{
    assert(dag_context);
    for (const auto & entry : res)
    {
        entry.second->collectRuntimeDetail();
    }
}
} // namespace DB