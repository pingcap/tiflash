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
#include <common/types.h>
#include <tipb/select.pb.h>

namespace DB
{
/// do not need be thread safe since it is only used in single thread env
struct ExecutionSummary
{
    UInt64 time_processed_ns;
    UInt64 num_produced_rows;
    UInt64 num_iterations;
    UInt64 concurrency;
    ExecutionSummary()
        : time_processed_ns(0)
        , num_produced_rows(0)
        , num_iterations(0)
        , concurrency(0)
    {}

    void merge(const ExecutionSummary & other, bool streaming_call);
};

class DAGResponseWriter
{
public:
    DAGResponseWriter(
        Int64 records_per_chunk_,
        DAGContext & dag_context_);
    void fillTiExecutionSummary(
        tipb::ExecutorExecutionSummary * execution_summary,
        ExecutionSummary & current,
        const String & executor_id,
        bool delta_mode);
    void addExecuteSummaries(tipb::SelectResponse & response, bool delta_mode);
    /// 'last_null_block' is used for batch writer to flush cached blocks, when it is true, 'block' operator bool() should return false
    virtual void write(const Block & block, bool last_null_block) = 0;
    virtual void finishWrite() = 0;
    virtual ~DAGResponseWriter() = default;
    const DAGContext & dagContext() const { return dag_context; }

protected:
    Int64 records_per_chunk;
    DAGContext & dag_context;
    std::unordered_map<String, ExecutionSummary> previous_execution_stats;
    std::unordered_set<String> local_executors;
};

} // namespace DB
