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
#include <Flash/Pipeline/Schedule/ThreadPool/TaskThreadPoolMetrics.h>

#include <atomic>

namespace DB
{
#define INC_METRIC(metric_name, value)                                                                    \
    do                                                                                                    \
    {                                                                                                     \
        if constexpr (is_cpu)                                                                             \
        {                                                                                                 \
            thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_cpu_##metric_name); \
            metrics.Increment(value);                                                                     \
        }                                                                                                 \
        else                                                                                              \
        {                                                                                                 \
            thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_io_##metric_name);  \
            metrics.Increment(value);                                                                     \
        }                                                                                                 \
    } while (0)

#define DEC_METRIC(metric_name, value)                                                                    \
    do                                                                                                    \
    {                                                                                                     \
        if constexpr (is_cpu)                                                                             \
        {                                                                                                 \
            thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_cpu_##metric_name); \
            metrics.Decrement(value);                                                                     \
        }                                                                                                 \
        else                                                                                              \
        {                                                                                                 \
            thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_io_##metric_name);  \
            metrics.Decrement(value);                                                                     \
        }                                                                                                 \
    } while (0)

#define SET_METRIC(metric_name, value)                                                                    \
    do                                                                                                    \
    {                                                                                                     \
        if constexpr (is_cpu)                                                                             \
        {                                                                                                 \
            thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_cpu_##metric_name); \
            metrics.Set(value);                                                                           \
        }                                                                                                 \
        else                                                                                              \
        {                                                                                                 \
            thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_io_##metric_name);  \
            metrics.Set(value);                                                                           \
        }                                                                                                 \
    } while (0)

template <bool is_cpu>
TaskThreadPoolMetrics<is_cpu>::TaskThreadPoolMetrics()
{
    SET_METRIC(pending_tasks_count, 0);
    SET_METRIC(executing_tasks_count, 0);
    SET_METRIC(task_thread_pool_size, 0);
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
void TaskThreadPoolMetrics<is_cpu>::elapsedPendingTime(TaskPtr & task)
{
    if constexpr (is_cpu)
        task->profile_info.elapsedCPUPendingTime();
    else
        task->profile_info.elapsedIOPendingTime();
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
void TaskThreadPoolMetrics<is_cpu>::addExecuteTime(TaskPtr & task, UInt64 value)
{
    if constexpr (is_cpu)
        task->profile_info.addCPUExecuteTime(value);
    else
        task->profile_info.addIOExecuteTime(value);
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

template class TaskThreadPoolMetrics<true>;
template class TaskThreadPoolMetrics<false>;

#undef INC_METRIC
#undef DEC_METRIC
#undef SET_METRIC

} // namespace DB
