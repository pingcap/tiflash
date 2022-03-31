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
std::unique_ptr<DAGExpressionAnalyzer> handleTableScan(
    Context & context,
    const TiDBTableScan & table_scan,
    const String & filter_executor_id,
    const std::vector<const tipb::Expr *> & conditions,
    DAGPipeline & pipeline,
    size_t max_streams);
} // namespace DB::TableScanInterpreterHelper