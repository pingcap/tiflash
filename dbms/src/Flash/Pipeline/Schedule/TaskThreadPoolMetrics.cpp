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

#include <Common/TiFlashMetrics.h>
#include <Flash/Pipeline/Schedule/TaskThreadPoolMetrics.h>

#include <atomic>

namespace DB
{
#define INC_METRIC(metric_name, value)                                                       \
    do                                                                                       \
    {                                                                                        \
        if constexpr (is_cpu)                                                                \
        {                                                                                    \
            GET_METRIC(tiflash_pipeline_scheduler, type_cpu_##metric_name).Increment(value); \
        }                                                                                    \
        else                                                                                 \
        {                                                                                    \
            GET_METRIC(tiflash_pipeline_scheduler, type_io_##metric_name).Increment(value);  \
        }                                                                                    \
    } while (0)

#define DEC_METRIC(metric_name, value)                                                       \
    do                                                                                       \
    {                                                                                        \
        if constexpr (is_cpu)                                                                \
        {                                                                                    \
            GET_METRIC(tiflash_pipeline_scheduler, type_cpu_##metric_name).Decrement(value); \
        }                                                                                    \
        else                                                                                 \
        {                                                                                    \
            GET_METRIC(tiflash_pipeline_scheduler, type_io_##metric_name).Decrement(value);  \
        }                                                                                    \
    } while (0)

#define SET_METRIC(metric_name, value)                                                 \
    do                                                                                 \
    {                                                                                  \
        if constexpr (is_cpu)                                                          \
        {                                                                              \
            GET_METRIC(tiflash_pipeline_scheduler, type_cpu_##metric_name).Set(value); \
        }                                                                              \
        else                                                                           \
        {                                                                              \
            GET_METRIC(tiflash_pipeline_scheduler, type_io_##metric_name).Set(value);  \
        }                                                                              \
    } while (0)

// TODO support more metrics after profile info of task has supported.
template <bool is_cpu>
TaskThreadPoolMetrics<is_cpu>::TaskThreadPoolMetrics()
{
    SET_METRIC(pending_tasks_count, 0);
    SET_METRIC(executing_tasks_count, 0);
    SET_METRIC(task_thread_pool_size, 0);
    SET_METRIC(max_execution_time_ms_of_a_round, 0);
}

template <bool is_cpu>
void TaskThreadPoolMetrics<is_cpu>::incPendingTask(size_t task_count)
{
    INC_METRIC(pending_tasks_count, task_count);
}

template <bool is_cpu>
void TaskThreadPoolMetrics<is_cpu>::decPendingTask()
{
    DEC_METRIC(pending_tasks_count, 1);
}

template <bool is_cpu>
void TaskThreadPoolMetrics<is_cpu>::incExecutingTask()
{
    INC_METRIC(executing_tasks_count, 1);
}

template <bool is_cpu>
void TaskThreadPoolMetrics<is_cpu>::decExecutingTask()
{
    DEC_METRIC(executing_tasks_count, 1);
}

template <bool is_cpu>
void TaskThreadPoolMetrics<is_cpu>::incThreadCnt()
{
    INC_METRIC(task_thread_pool_size, 1);
}

template <bool is_cpu>
void TaskThreadPoolMetrics<is_cpu>::decThreadCnt()
{
    DEC_METRIC(task_thread_pool_size, 1);
}

template <bool is_cpu>
void TaskThreadPoolMetrics<is_cpu>::updateTaskMaxtimeOnRound(uint64_t max_execution_time_ns)
{
    while (true)
    {
        auto cur_max_ns = max_execution_time_ns_of_a_round.load();
        if (max_execution_time_ns <= cur_max_ns)
            return;
        if (max_execution_time_ns_of_a_round.compare_exchange_strong(cur_max_ns, max_execution_time_ns))
        {
            // Here still take from `max_execution_time_ns_of_a_round` instead of `max_execution_time_ns`.
            // Otherwise, for a < b, if there is such an execution sequence
            // 1. max_execution_time_ns_of_a_round.set(a)
            // 2. max_execution_time_ns_of_a_round.set(b)
            // 3. setMaxTimeMetrics(b)
            // 4. setMaxTimeMetrics(a)
            // The max time in metrics will be a instead of b.
            SET_METRIC(max_execution_time_ms_of_a_round, max_execution_time_ns_of_a_round.load() / 1000.0);
            return;
        }
    }
}

template class TaskThreadPoolMetrics<true>;
template class TaskThreadPoolMetrics<false>;

#undef INC_METRIC
#undef DEC_METRIC
#undef SET_METRIC

} // namespace DB
