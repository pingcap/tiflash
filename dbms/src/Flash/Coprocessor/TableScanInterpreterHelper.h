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

#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Interpreters/Context.h>

namespace DB::TableScanInterpreterHelper
{
void executeRemoteQueryImpl(
    const Context & context,
    DAGPipeline & pipeline,
    std::vector<RemoteRequest> & remote_requests);

void executeCastAfterTableScan(
    const Context & context,
    DAGExpressionAnalyzer & analyzer,
    const std::vector<ExtraCastAfterTSMode> & is_need_add_cast_column,
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline);

void executePushedDownFilter(
    const std::vector<const tipb::Expr *> & conditions,
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline);
} // namespace DB::TableScanInterpreterHelper