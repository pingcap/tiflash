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

#pragma once

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/ExecutionSummary.h>

namespace DB
{
class ExecutionSummaryCollector
{
public:
    explicit ExecutionSummaryCollector(
        DAGContext & dag_context_)
        : dag_context(dag_context_)
    {
        for (auto & p : dag_context.getProfileStreamsMap())
        {
            local_executors.insert(p.first);
        }
    }

    void addExecuteSummaries(tipb::SelectResponse & response);

    tipb::SelectResponse genExecutionSummaryResponse();

private:
    void fillTiExecutionSummary(
        tipb::ExecutorExecutionSummary * execution_summary,
        ExecutionSummary & current,
        const String & executor_id) const;

private:
    DAGContext & dag_context;
    std::unordered_set<String> local_executors;
};
} // namespace DB
