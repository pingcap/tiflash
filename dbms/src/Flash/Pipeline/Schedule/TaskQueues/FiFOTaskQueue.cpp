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

#include <Flash/Pipeline/Schedule/TaskQueues/FiFOTaskQueue.h>
#include <assert.h>
#include <common/likely.h>

namespace DB
{
bool FIFOTaskQueue::take(TaskPtr & task) noexcept
{
    assert(!task);
    {
        std::unique_lock lock(mu);
        while (true)
        {
            if (unlikely(is_closed))
                return false;
            if (!task_queue.empty())
                break;
            cv.wait(lock);
        }

        task = std::move(task_queue.front());
        task_queue.pop_front();
    }
    assert(task);
    return true;
}

bool FIFOTaskQueue::empty() noexcept
{
    std::lock_guard lock(mu);
    return task_queue.empty();
}

void FIFOTaskQueue::close()
{
    {
        std::lock_guard lock(mu);
        is_closed = true;
    }
    cv.notify_all();
}

void FIFOTaskQueue::submit(TaskPtr && task) noexcept
{
    assert(task);
    {
        std::lock_guard lock(mu);
        task_queue.push_back(std::move(task));
    }
    cv.notify_one();
}

void FIFOTaskQueue::submit(std::vector<TaskPtr> & tasks) noexcept
{
    if (tasks.empty())
        return;

    std::lock_guard lock(mu);
    for (auto & task : tasks)
    {
        assert(task);
        task_queue.push_back(std::move(task));
        cv.notify_one();
    }
}
} // namespace DB
