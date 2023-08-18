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

#pragma once

#include <Storages/DeltaMerge/ScanContext.h>
#include <common/types.h>
#include <tipb/select.pb.h>

#include <memory>

namespace DB
{

struct BaseRuntimeStatistics;
/// do not need be thread safe since it is only used in single thread env
struct ExecutionSummary
{
    UInt64 time_processed_ns = 0;
    UInt64 num_produced_rows = 0;
    UInt64 num_iterations = 0;
    UInt64 concurrency = 0;

    DM::ScanContextPtr scan_context = std::make_shared<DB::DM::ScanContext>();

    ExecutionSummary() = default;

    void merge(const ExecutionSummary & other);
    void merge(const tipb::ExecutorExecutionSummary & other);
    void fill(const BaseRuntimeStatistics & other);
    void init(const tipb::ExecutorExecutionSummary & other);
};

} // namespace DB
