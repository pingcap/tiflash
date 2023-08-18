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

#include <Flash/Pipeline/Schedule/Reactor/WaitingTaskList.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <assert.h>
#include <common/likely.h>

namespace DB
{
bool WaitingTaskList::take(std::list<TaskPtr> & local_waiting_tasks)
{
    {
        std::unique_lock lock(mu);
        while (true)
        {
            if (!waiting_tasks.empty())
                break;
            if (unlikely(is_finished))
                return false;
            cv.wait(lock);
        }

        local_waiting_tasks.splice(local_waiting_tasks.end(), waiting_tasks);
    }
    assert(!local_waiting_tasks.empty());
    return true;
}

bool WaitingTaskList::tryTake(std::list<TaskPtr> & local_waiting_tasks)
{
    std::lock_guard lock(mu);
    if (waiting_tasks.empty())
        return !is_finished;
    local_waiting_tasks.splice(local_waiting_tasks.end(), waiting_tasks);
    return true;
}

void WaitingTaskList::finish()
{
    {
        std::lock_guard lock(mu);
        is_finished = true;
    }
    cv.notify_all();
}

void WaitingTaskList::submit(TaskPtr && task)
{
    if unlikely (is_finished)
    {
        FINALIZE_TASK(task);
        return;
    }

    {
        std::lock_guard lock(mu);
        assert(task);
        waiting_tasks.emplace_back(std::move(task));
    }
    cv.notify_one();
}

void WaitingTaskList::submit(std::list<TaskPtr> & tasks)
{
    if unlikely (is_finished)
    {
        FINALIZE_TASKS(tasks);
        return;
    }

    if (tasks.empty())
        return;
    {
        std::lock_guard lock(mu);
        waiting_tasks.splice(waiting_tasks.end(), tasks);
    }
    cv.notify_one();
}
} // namespace DB
