// Copyright 2023 PingCAP, Ltd.
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

#include <Common/TiFlashMetrics.h>
#include <Flash/Pipeline/Schedule/TaskExecutionMetrics.h>

namespace DB
{
TaskExecutionMetrics::TaskExecutionMetrics()
{
    GET_METRIC(tiflash_pipeline_scheduler, type_max_execution_time_ms_of_a_round).Set(0);
}

void TaskExecutionMetrics::updateOnRound(uint64_t execution_time_ns)
{
    while (true)
    {
        auto cur_max_ns = max_execution_time_ns_of_a_round.load();
        if (execution_time_ns <= cur_max_ns)
            return;
        if (max_execution_time_ns_of_a_round.compare_exchange_strong(cur_max_ns, execution_time_ns))
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_max_execution_time_ms_of_a_round).Set(max_execution_time_ns_of_a_round.load() / 1000);
            return;
        }
    }
}
} // namespace DB
