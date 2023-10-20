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

#include <Common/DynamicThreadPool.h>
#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>

#include <exception>

namespace DB
{
namespace FailPoints
{
extern const char exception_new_dynamic_thread[];
} // namespace FailPoints

DynamicThreadPool::~DynamicThreadPool()
{
    for (auto & queue : fixed_queues)
        queue->finish();

    for (auto & thread : fixed_threads)
        thread.join();

    {
        std::unique_lock lock(dynamic_mutex);
        in_destructing = true;
        // do not need to detach node here
        for (auto * node = dynamic_idle_head.next; node != &dynamic_idle_head; node = node->next)
            node->cv.notify_one();
    }

    // TODO: maybe use a latch is more elegant.
    while (alive_dynamic_threads.load() != 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

DynamicThreadPool::ThreadCount DynamicThreadPool::threadCount() const
{
    ThreadCount cnt;
    cnt.fixed = fixed_threads.size();
    cnt.dynamic = alive_dynamic_threads.load();
    return cnt;
}

void DynamicThreadPool::init(size_t initial_size)
{
    fixed_queues.reserve(initial_size);
    fixed_threads.reserve(initial_size);
    for (size_t i = 0; i < initial_size; ++i)
    {
        fixed_queues.emplace_back(std::make_unique<Queue>(1)); // each Queue will only contain at most 1 task.
        idle_fixed_queues.push(fixed_queues.back().get());
        fixed_threads.emplace_back(
            ThreadFactory::newThread(false, "FixedThread", &DynamicThreadPool::fixedWork, this, i));
    }
}

void DynamicThreadPool::scheduleTask(TaskPtr task)
{
    if (!scheduledToFixedThread(task) && !scheduledToExistedDynamicThread(task))
        scheduledToNewDynamicThread(task);
}

bool DynamicThreadPool::scheduledToFixedThread(TaskPtr & task)
{
    Queue * queue = nullptr;
    if (idle_fixed_queues.pop(queue))
    {
        queue->push(std::move(task));
        return true;
    }
    return false;
}

bool DynamicThreadPool::scheduledToExistedDynamicThread(TaskPtr & task)
{
    std::unique_lock lock(dynamic_mutex);
    if (dynamic_idle_head.isSingle())
        return false;
    DynamicNode * node = dynamic_idle_head.next;
    // detach node to avoid assigning two tasks to node.
    node->detach();
    node->task = std::move(task);
    node->cv.notify_one();
    return true;
}

void DynamicThreadPool::scheduledToNewDynamicThread(TaskPtr & task)
{
    std::thread t = newDynamcThread(task);
    t.detach();
}

std::thread DynamicThreadPool::newDynamcThread(TaskPtr & task)
{
    alive_dynamic_threads.fetch_add(1);
    try
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_new_dynamic_thread);
        return ThreadFactory::newThread(false, "DynamicThread", &DynamicThreadPool::dynamicWork, this, std::move(task));
    }
    catch (...)
    {
        alive_dynamic_threads.fetch_sub(1);
        std::rethrow_exception(std::current_exception());
    }
}

void DynamicThreadPool::executeTask(TaskPtr & task)
{
    UPDATE_CUR_AND_MAX_METRIC(tiflash_thread_count, type_active_threads_of_thdpool, type_max_active_threads_of_thdpool);
    task->execute();
    task.reset();
}

void DynamicThreadPool::fixedWork(size_t index)
{
    UPDATE_CUR_AND_MAX_METRIC(tiflash_thread_count, type_total_threads_of_thdpool, type_max_threads_of_thdpool);
    Queue * queue = fixed_queues[index].get();
    while (true)
    {
        TaskPtr task;
        queue->pop(task);
        if (!task)
            break;
        executeTask(task);

        idle_fixed_queues.push(queue);
    }
}

void DynamicThreadPool::dynamicWork(TaskPtr initial_task)
{
    {
        UPDATE_CUR_AND_MAX_METRIC(tiflash_thread_count, type_total_threads_of_thdpool, type_max_threads_of_thdpool);
        executeTask(initial_task);

        DynamicNode node;
        while (true)
        {
            {
                std::unique_lock lock(dynamic_mutex);
                if (in_destructing)
                    break;
                // attach to just after head to reuse hot threads so that cold threads have chance to exit
                node.appendTo(&dynamic_idle_head);
                node.cv.wait_for(lock, dynamic_auto_shrink_cooldown);
                node.detach();
            }

            if (!node.task) // may be timeout or cancelled
                break;
            executeTask(node.task);
        }
    }
    // must decrease counter after scope of `UPDATE_CUR_AND_MAX_METRIC`
    // to avoid potential data race (#4595)
    alive_dynamic_threads.fetch_sub(1);
}

std::unique_ptr<DynamicThreadPool> DynamicThreadPool::global_instance;
} // namespace DB
