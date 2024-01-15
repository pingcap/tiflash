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

#include <Common/Exception.h>
#include <Flash/Coprocessor/RemoteExecutionSummary.h>
#include <common/likely.h>

namespace DB
{
void RemoteExecutionSummary::merge(const RemoteExecutionSummary & other)
{
    for (const auto & p : other.execution_summaries)
    {
        const auto & executor_id = p.first;
        auto it = execution_summaries.find(executor_id);
        if (unlikely(it == execution_summaries.end()))
        {
            execution_summaries[executor_id] = p.second;
        }
        else
        {
            it->second.merge(p.second);
        }

        ru_consumption = mergeRUConsumption(ru_consumption, other.ru_consumption);
    }
}

void RemoteExecutionSummary::add(tipb::SelectResponse & resp)
{
    if (unlikely(resp.execution_summaries_size() == 0))
        return;

    std::unique_ptr<resource_manager::Consumption> remote_ru_consumption;
    for (const auto & execution_summary : resp.execution_summaries())
    {
        if (likely(execution_summary.has_executor_id()))
        {
            if (execution_summary.has_ru_consumption())
            {
                RUNTIME_CHECK_MSG(!remote_ru_consumption, "number ru consumption in MPPTask should be one");
                remote_ru_consumption = std::make_unique<resource_manager::Consumption>();
                if unlikely (!(remote_ru_consumption->ParseFromString(execution_summary.ru_consumption())))
                    throw Exception("failed to parse ru consumption from remote execution summary");

                ru_consumption = mergeRUConsumption(ru_consumption, *remote_ru_consumption);
            }

            const auto & executor_id = execution_summary.executor_id();
            auto it = execution_summaries.find(executor_id);
            if (unlikely(it == execution_summaries.end()))
            {
                execution_summaries[executor_id].init(execution_summary);
            }
            else
            {
                it->second.merge(execution_summary);
            }
        }
    }
}

resource_manager::Consumption mergeRUConsumption(
    const resource_manager::Consumption & left,
    const resource_manager::Consumption & right)
{
    // TiFlash only support read related RU for now.
    // So ignore merge other fields.
    resource_manager::Consumption sum;
    sum.set_r_r_u(left.r_r_u() + right.r_r_u());
    sum.set_read_bytes(left.read_bytes() + right.read_bytes());
    sum.set_total_cpu_time_ms(left.total_cpu_time_ms() + right.total_cpu_time_ms());
    return sum;
}
} // namespace DB
