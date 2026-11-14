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

#include <Common/Exception.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskWaiter.h>
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
        case NotifyType::WAIT_ON_CTE_READ:
            GET_METRIC(tiflash_pipeline_wait_on_notify_tasks, type_type_wait_on_cte_read).Increment(change);
            break;
        case NotifyType::WAIT_ON_NOTHING:
            throw Exception("task notify type should be set before register or notify");
            break;
        }
    }

    inline void registerTask(TaskPtr && task)
    {
        assert(task && task->getStatus() == ExecTaskStatus::WAIT_FOR_NOTIFY);
        auto waiter = task->getWaiter();
        RUNTIME_CHECK_MSG(waiter, "Task waiter must be initialized by registerTaskToFuture before registration");
        std::lock_guard lock(mu);
        tasks.push_back(std::move(waiter));
        // Account for the task only after enqueueing succeeds, while notification is still excluded.
        tasks.back()->setTask(std::move(task));
    }

    inline void notifyOne()
    {
        while (true)
        {
            TaskWaiterPtr waiter;
            {
                std::lock_guard lock(mu);
                if (tasks.empty())
                    return;
                waiter = std::move(tasks.front());
                tasks.pop_front();
            }
            // A stopped task may already have left this queue. Keep looking for a live waiter.
            if (waiter->notify())
                return;
        }
    }

    inline void notifyAll()
    {
        std::deque<TaskWaiterPtr> ready_waiters;
        {
            std::lock_guard lock(mu);
            ready_waiters.swap(tasks);
        }
        for (const auto & waiter : ready_waiters)
            waiter->notify();
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
    std::deque<TaskWaiterPtr> tasks;
};
} // namespace DB
