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

#pragma once

#include <Common/TiFlashMetrics.h>

#include <atomic>

namespace DB
{
// TODO support more metrics after profile info of task has supported.
template <bool is_cpu>
class TaskThreadPoolMetrics
{
public:
    TaskThreadPoolMetrics()
    {
        if constexpr (is_cpu)
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_cpu_pending_tasks_count).Set(0);
            GET_METRIC(tiflash_pipeline_scheduler, type_cpu_executing_tasks_count).Set(0);
            GET_METRIC(tiflash_pipeline_scheduler, type_cpu_task_thread_pool_size).Set(0);
        }
        else
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_io_pending_tasks_count).Set(0);
            GET_METRIC(tiflash_pipeline_scheduler, type_io_executing_tasks_count).Set(0);
            GET_METRIC(tiflash_pipeline_scheduler, type_io_task_thread_pool_size).Set(0);
        }
        setMaxTimeMetrics(0);
    }

    void incPendingTask(size_t task_count)
    {
        if constexpr (is_cpu)
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_cpu_pending_tasks_count).Increment(task_count);
        }
        else
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_io_pending_tasks_count).Increment(task_count);
        }
    }

    void decPendingTask()
    {
        if constexpr (is_cpu)
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_cpu_pending_tasks_count).Decrement();
        }
        else
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_io_pending_tasks_count).Decrement();
        }
    }

    void incExecutingTask()
    {
        if constexpr (is_cpu)
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_cpu_executing_tasks_count).Increment();
        }
        else
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_io_executing_tasks_count).Increment();
        }
    }

    void decExecutingTask()
    {
        if constexpr (is_cpu)
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_cpu_executing_tasks_count).Decrement();
        }
        else
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_io_executing_tasks_count).Decrement();
        }
    }

    void incThreadCnt()
    {
        if constexpr (is_cpu)
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_cpu_task_thread_pool_size).Increment();
        }
        else
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_cpu_task_thread_pool_size).Increment();
        }
    }

    void decThreadCnt()
    {
        if constexpr (is_cpu)
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_io_task_thread_pool_size).Decrement();
        }
        else
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_io_task_thread_pool_size).Decrement();
        }
    }

    void updateTaskMaxtimeOnRound(uint64_t max_execution_time_ns)
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
                setMaxTimeMetrics(max_execution_time_ns_of_a_round.load() / 1000.0);
                return;
            }
        }
    }

private:
    void setMaxTimeMetrics(double max_execution_time)
    {
        if constexpr (is_cpu)
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_max_cpu_execution_time_ms_of_a_round).Set(max_execution_time);
        }
        else
        {
            GET_METRIC(tiflash_pipeline_scheduler, type_max_io_execution_time_ms_of_a_round).Set(max_execution_time);
        }
    }

private:
    std::atomic_uint64_t max_execution_time_ns_of_a_round{0};
};
} // namespace DB
