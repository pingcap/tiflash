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

#include <Flash/Coprocessor/ExecutionSummary.h>
#include <Flash/Statistics/BaseRuntimeStatistics.h>
#include <Storages/DeltaMerge/ScanContext.h>

namespace DB
{
ExecutionSummary::ExecutionSummary()
    : scan_context(std::make_shared<DM::ScanContext>())
{}

void ExecutionSummary::merge(const ExecutionSummary & other)
{
    time_processed_ns = std::max(time_processed_ns, other.time_processed_ns);
    num_produced_rows += other.num_produced_rows;
    num_iterations += other.num_iterations;
    concurrency += other.concurrency;
    scan_context->merge(*other.scan_context);
}

void ExecutionSummary::merge(const tipb::ExecutorExecutionSummary & other)
{
    time_processed_ns = std::max(time_processed_ns, other.time_processed_ns());
    num_produced_rows += other.num_produced_rows();
    num_iterations += other.num_iterations();
    concurrency += other.concurrency();
    scan_context->merge(other.tiflash_scan_context());
}

void ExecutionSummary::fill(const BaseRuntimeStatistics & other)
{
    time_processed_ns = other.execution_time_ns;
    num_produced_rows = other.rows;
    num_iterations = other.blocks;
    concurrency = other.concurrency;
}

void ExecutionSummary::init(const tipb::ExecutorExecutionSummary & other)
{
    time_processed_ns = other.time_processed_ns();
    num_produced_rows = other.num_produced_rows();
    num_iterations = other.num_iterations();
    concurrency = other.concurrency();
    scan_context->deserialize(other.tiflash_scan_context());
}
} // namespace DB
