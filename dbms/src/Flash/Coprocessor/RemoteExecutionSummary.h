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
#include <kvproto/resource_manager.pb.h>
#include <tipb/select.pb.h>

#include <unordered_map>

namespace DB
{
struct RemoteExecutionSummary
{
    void merge(const RemoteExecutionSummary & other);

    void add(tipb::SelectResponse & resp);

    // <executor_id, ExecutionSummary>
    std::unordered_map<String, ExecutionSummary> execution_summaries;
    resource_manager::Consumption ru_consumption;
};

resource_manager::Consumption mergeRUConsumption(
    const resource_manager::Consumption & left,
    const resource_manager::Consumption & right)
{
    // TiFlash only support read related RU for now.
    resource_manager::Consumption sum;
    sum.set_r_r_u(left.r_r_u() + right.r_r_u());
    sum.set_read_bytes(left.read_bytes() + right.read_bytes());
    sum.set_total_cpu_time_ms(left.total_cpu_time_ms() + right.total_cpu_time_ms());
    return sum;
}
} // namespace DB
