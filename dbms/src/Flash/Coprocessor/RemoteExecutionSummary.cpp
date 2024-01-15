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
    }
}

void RemoteExecutionSummary::add(tipb::SelectResponse & resp)
{
    if (unlikely(resp.execution_summaries_size() == 0))
        return;

    for (const auto & execution_summary : resp.execution_summaries())
    {
        if (likely(execution_summary.has_executor_id()))
        {
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
} // namespace DB
