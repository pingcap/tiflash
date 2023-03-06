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

#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/ExecutionSummary.h>
#include <Storages/DeltaMerge/ScanContext.h>

namespace DB
{
class DAGContext;

class ExecutionSummaryCollector
{
public:
    ExecutionSummaryCollector(
        DAGContext & dag_context_,
        bool enable_pipeline_,
        bool fill_executor_id_ = false)
        : dag_context(dag_context_)
        , enable_pipeline(enable_pipeline_)
        , fill_executor_id(fill_executor_id_)
    {}

    void addExecuteSummaries(tipb::SelectResponse & response);

    void addExecuteSummariesForPipeline(tipb::SelectResponse & response);

    tipb::SelectResponse genExecutionSummaryResponse();

private:
    void fillTiExecutionSummary(
        tipb::ExecutorExecutionSummary * execution_summary,
        ExecutionSummary & current,
        const String & executor_id) const;

    void fillLocalExecutionSummary(
        tipb::SelectResponse & response,
        const String & executor_id,
        const BlockInputStreams & streams,
        const std::unordered_map<String, DM::ScanContextPtr> & scan_context_map) const;

    // fill the ExecutionSummary for one executor and return the time spend on the executor and it's children.
    void fillLocalExecutionSummaryForPipeline(
        tipb::SelectResponse & response,
        ExecutionSummary & current,
        bool empty_executor,
        const String & executor_id,
        const ExecutorProfileInfo & executor_profile,
        const std::unordered_map<String, DM::ScanContextPtr> & scan_context_map) const;

private:
    DAGContext & dag_context;
    bool enable_pipeline;

    /// for testing execution summary of list-based DagRequest
    bool fill_executor_id;
};
} // namespace DB
