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

#include <Flash/Pipeline/Schedule/TaskQueues/FIFOTaskQueue.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <common/likely.h>

namespace DB
{
FIFOTaskQueue::~FIFOTaskQueue()
{
    RUNTIME_ASSERT(task_queue.empty(), logger, "all task should be taken before it is destructed");
}

bool FIFOTaskQueue::take(TaskPtr & task)
{
    std::unique_lock lock(mu);
    while (true)
    {
        if (!task_queue.empty())
            break;
        if (unlikely(is_finished))
            return false;
        cv.wait(lock);
    }

    task = std::move(task_queue.front());
    task_queue.pop_front();
    return true;
}

bool FIFOTaskQueue::empty() const
{
    std::lock_guard lock(mu);
    return task_queue.empty();
}

void FIFOTaskQueue::finish()
{
    {
        std::lock_guard lock(mu);
        is_finished = true;
    }
    cv.notify_all();
}

void FIFOTaskQueue::submit(TaskPtr && task)
{
    if unlikely (is_finished)
    {
        FINALIZE_TASK(task);
        return;
    }

    {
        std::lock_guard lock(mu);
        task_queue.push_back(std::move(task));
    }
    cv.notify_one();
}

void FIFOTaskQueue::submit(std::vector<TaskPtr> & tasks)
{
    if unlikely (is_finished)
    {
        FINALIZE_TASKS(tasks);
        return;
    }

    if (tasks.empty())
        return;
    std::lock_guard lock(mu);
    for (auto & task : tasks)
    {
        task_queue.push_back(std::move(task));
        cv.notify_one();
    }
}
} // namespace DB
