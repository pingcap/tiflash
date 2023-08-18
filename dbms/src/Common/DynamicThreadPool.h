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

#include <Common/ExecutableTask.h>
#include <Common/MPMCQueue.h>
#include <Common/ThreadFactory.h>
#include <Common/packTask.h>
#include <boost_wrapper/lockfree_queue.h>

#include <chrono>
#include <future>
#include <thread>

namespace DB
{
class DynamicThreadPool
{
private:
    using TaskPtr = std::unique_ptr<IExecutableTask>;
    using Queue = MPMCQueue<TaskPtr>;

    // used for dynamic threads
    struct DynamicNode : public SimpleIntrusiveNode<DynamicNode>
    {
        std::condition_variable cv;
        TaskPtr task;
    };

public:
    template <typename Duration>
    DynamicThreadPool(size_t initial_size, Duration auto_shrink_cooldown)
        : dynamic_auto_shrink_cooldown(std::chrono::duration_cast<std::chrono::nanoseconds>(auto_shrink_cooldown))
        , idle_fixed_queues(initial_size)
    {
        init(initial_size);
    }

    ~DynamicThreadPool();

    // wrap func into a std::packaged_task so users can get the status of execution via the returned future.
    template <typename Func, typename... Args>
    auto schedule(bool propagate_memory_tracker, Func && func, Args &&... args)
    {
        auto task = packTask(propagate_memory_tracker, std::forward<Func>(func), std::forward<Args>(args)...);
        auto future = task.get_future();
        scheduleTask(std::make_unique<ExecutableTask<decltype(task)>>(std::move(task)));
        return future;
    }

    // wrap func into a lambda and users can't get the status of execution.
    // NOTE: exceptions thrown from func might cause the process terminate.
    template <typename Func, typename... Args>
    void scheduleRaw(bool propagate_memory_tracker, Func && func, Args &&... args)
    {
        auto invocable = wrapInvocable(propagate_memory_tracker, std::forward<Func>(func), std::forward<Args>(args)...);
        scheduleTask(std::make_unique<ExecutableTask<decltype(invocable)>>(std::move(invocable)));
    }

    struct ThreadCount
    {
        Int32 fixed = 0;
        Int32 dynamic = 0;
    };

    ThreadCount threadCount() const;

    static std::unique_ptr<DynamicThreadPool> global_instance;

private:
    void init(size_t initial_size);
    void scheduleTask(TaskPtr task);
    bool scheduledToFixedThread(TaskPtr & task);
    bool scheduledToExistedDynamicThread(TaskPtr & task);
    void scheduledToNewDynamicThread(TaskPtr & task);

    inline std::thread newDynamcThread(TaskPtr & task);

    void fixedWork(size_t index);
    void dynamicWork(TaskPtr initial_task);

    static void executeTask(TaskPtr & task);

    const std::chrono::nanoseconds dynamic_auto_shrink_cooldown;

    std::vector<std::thread> fixed_threads;
    // Each fixed thread interacts with outside via a Queue.
    std::vector<std::unique_ptr<Queue>> fixed_queues;
    boost::lockfree::queue<Queue *> idle_fixed_queues;

    std::mutex dynamic_mutex;
    DynamicNode dynamic_idle_head;
    bool in_destructing = false;

    std::atomic<Int64> alive_dynamic_threads = 0;
};
} // namespace DB
