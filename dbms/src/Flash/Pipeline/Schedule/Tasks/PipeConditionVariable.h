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

#include <Common/TiFlashMetrics.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <prometheus/gauge.h>

#include <deque>

namespace DB
{
class PipeConditionVariable
{
public:
    static inline void updateTaskNotifyWaitMetrics(NotifyType type, Int64 change)
    {
        switch (type)
        {
        case NotifyType::WAIT_ON_TABLE_SCAN_READ:
            GET_METRIC(tiflash_pipeline_wait_on_notify_tasks, type_wait_on_table_scan_read).Increment(change);
            break;
        case NotifyType::WAIT_ON_SHARED_QUEUE_READ:
            GET_METRIC(tiflash_pipeline_wait_on_notify_tasks, type_wait_on_shared_queue_read).Increment(change);
            break;
        case NotifyType::WAIT_ON_SPILL_BUCKET_READ:
            GET_METRIC(tiflash_pipeline_wait_on_notify_tasks, type_wait_on_spill_bucket_read).Increment(change);
            break;
        case NotifyType::WAIT_ON_GRPC_RECV_READ:
            GET_METRIC(tiflash_pipeline_wait_on_notify_tasks, type_wait_on_grpc_recv_read).Increment(change);
            break;
        case NotifyType::WAIT_ON_TUNNEL_SENDER_WRITE:
            GET_METRIC(tiflash_pipeline_wait_on_notify_tasks, type_wait_on_tunnel_sender_write).Increment(change);
            break;
        case NotifyType::WAIT_ON_JOIN_BUILD_FINISH:
            GET_METRIC(tiflash_pipeline_wait_on_notify_tasks, type_wait_on_join_build).Increment(change);
            break;
        case NotifyType::WAIT_ON_JOIN_PROBE_FINISH:
            GET_METRIC(tiflash_pipeline_wait_on_notify_tasks, type_wait_on_join_probe).Increment(change);
            break;
        case NotifyType::WAIT_ON_RESULT_QUEUE_WRITE:
            GET_METRIC(tiflash_pipeline_wait_on_notify_tasks, type_wait_on_result_queue_write).Increment(change);
            break;
        case NotifyType::WAIT_ON_SHARED_QUEUE_WRITE:
            GET_METRIC(tiflash_pipeline_wait_on_notify_tasks, type_wait_on_shared_queue_write).Increment(change);
            break;
        case NotifyType::WAIT_ON_NOTHING:
            throw Exception("task notify type should be set before register or notify");
            break;
        }
    }

    inline void registerTask(TaskPtr && task)
    {
        assert(task);
        auto type = task->getNotifyType();
        assert(task->getStatus() == ExecTaskStatus::WAIT_FOR_NOTIFY);
        {
            std::lock_guard lock(mu);
            tasks.push_back(std::move(task));
        }

        thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
        metrics.Increment();
        updateTaskNotifyWaitMetrics(type, 1);
    }

    inline void notifyOne()
    {
        TaskPtr task;
        {
            std::lock_guard lock(mu);
            if (tasks.empty())
                return;
            task = std::move(tasks.front());
            tasks.pop_front();
        }
        assert(task);
        auto type = task->getNotifyType();
        notifyTaskDirectly(std::move(task));

        thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
        metrics.Decrement();
        updateTaskNotifyWaitMetrics(type, -1);
    }

    inline void notifyAll()
    {
        std::deque<TaskPtr> cur_tasks;
        {
            std::lock_guard lock(mu);
            std::swap(cur_tasks, tasks);
        }
        size_t tasks_cnt = cur_tasks.size();
        while (!cur_tasks.empty())
        {
            auto cur_task = std::move(cur_tasks.front());
            auto type = cur_task->getNotifyType();
            notifyTaskDirectly(std::move(cur_task));
            updateTaskNotifyWaitMetrics(type, -1);
            cur_tasks.pop_front();
        }

        thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
        metrics.Decrement(tasks_cnt);
    }

    static inline void notifyTaskDirectly(TaskPtr && task)
    {
        assert(task);
        task->notify();
        assert(TaskScheduler::instance);
        TaskScheduler::instance->submit(std::move(task));
    }

private:
    std::mutex mu;
    std::deque<TaskPtr> tasks;
};
} // namespace DB
