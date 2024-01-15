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

#include <Flash/Coprocessor/ExecutionSummary.h>
#include <tipb/select.pb.h>

#include <unordered_map>

namespace DB
{
struct RemoteExecutionSummary
{
    void merge(const RemoteExecutionSummary & other);
    void mergeRUConsumption(const RemoteExecutionSummary &);

    void add(tipb::SelectResponse & resp);

    // <executor_id, ExecutionSummary>
    std::unordered_map<String, ExecutionSummary> execution_summaries;
    // ru consumption of remote MPPTask.
    resource_manager::Consumption ru_consumption;
};

resource_manager::Consumption mergeRUConsumption(
    const resource_manager::Consumption & left,
    const resource_manager::Consumption & right);
} // namespace DB
