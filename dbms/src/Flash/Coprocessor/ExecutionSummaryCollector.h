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
#include <Flash/Coprocessor/RemoteExecutionSummary.h>

namespace DB
{
class ExecutionSummaryCollector
{
public:
    explicit ExecutionSummaryCollector(
        DAGContext & dag_context_)
        : dag_context(dag_context_)
    {}

    void addExecuteSummaries(tipb::SelectResponse & response);

    tipb::SelectResponse genExecutionSummaryResponse();

private:
    void fillTiExecutionSummary(
        tipb::ExecutorExecutionSummary * execution_summary,
        ExecutionSummary & current,
        const String & executor_id) const;

    std::pair<RemoteExecutionSummary, RemoteExecutionSummary> getRemoteExecutionSummaries() const;

    void fillLocalExecutionSummary(
        tipb::SelectResponse & response,
        const String & executor_id,
        const BlockInputStreams & streams,
        const RemoteExecutionSummary & remote_read_execution_summary) const;

private:
    DAGContext & dag_context;
};
} // namespace DB
