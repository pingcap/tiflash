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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutionSummaryHelper.h>
#include <Storages/DeltaMerge/ScanContext.h>

namespace DB
{
void fillTiExecutionSummary(
    DAGContext & dag_context,
    tipb::ExecutorExecutionSummary * execution_summary,
    ExecutionSummary & current,
    const String & executor_id,
    bool force_fill_executor_id)
{
    execution_summary->set_time_processed_ns(current.time_processed_ns);
    execution_summary->set_num_produced_rows(current.num_produced_rows);
    execution_summary->set_num_iterations(current.num_iterations);
    execution_summary->set_concurrency(current.concurrency);
    execution_summary->mutable_tiflash_scan_context()->CopyFrom(current.scan_context->serialize());
    RUNTIME_CHECK(current.ru_consumption.SerializeToString(execution_summary->mutable_ru_consumption()));

    // tree-based executors will have executor_id.
    // In ut, list-based executor will have executor_id for result comparision.
    if (dag_context.dag_request.isTreeBased() || force_fill_executor_id)
        execution_summary->set_executor_id(executor_id);
}
} // namespace DB
