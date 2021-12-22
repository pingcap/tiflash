#pragma once

#include <Common/ExecutableTask.h>
#include <Common/MPMCQueue.h>
#include <Common/ThreadFactory.h>
#include <Common/packTask.h>

#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <future>

namespace DB
{
class DynamicThreadPool
{
private:
    using TaskPtr = std::unique_ptr<IExecutableTask>;
    using Queue = MPMCQueue<TaskPtr>;

    // used for dynamic threads
    struct DynamicNode
    {
        DynamicNode()
        {
            next = this;
            prev = this;
        }

        /// valid when call on a single node whose next and prev is itself.
        void prepend(DynamicNode * head)
        {
            next = head->next;
            prev = head;
            head->next->prev = this;
            head->next = this;
        }

        /// valid when call on a single node whose next and prev is itself.
        void detach()
        {
            prev->next = next;
            next->prev = prev;
            next = this;
            prev = this;
        }

        bool noFollowers() const
        {
            return next == prev;
        }

        DynamicNode * next;
        DynamicNode * prev;
        std::condition_variable cv;
        TaskPtr task;
    };

public:
    template <typename Duration>
    DynamicThreadPool(size_t initial_size, Duration auto_shrink_cooldown)
        : dynamic_auto_shrink_cooldown(std::chrono::duration_cast<std::chrono::nanoseconds>(auto_shrink_cooldown))
    {
        init();
    }

    ~DynamicThreadPool();

    template <typename Func, typename... Args>
    auto schedule(Func && func, Args &&... args)
    {
        auto task = packTask(std::forward<Func>(func), std::forward<Args>(args)...);
        auto future = task.get_future();
        scheduleTask(std::make_unique<ExecutableTask<decltype(task)>>(std::move(task)));
        return std::move(future);
    }

    struct ThreadCount
    {
        Int32 fixed = 0;
        Int32 dynamic = 0;
    };

    ThreadCount threadCount() const;

private:
    void init();
    void scheduleTask(TaskPtr task);
    bool scheduledToFixedThread(TaskPtr & task);
    bool scheduledToExistedDynamicThread(TaskPtr & task);
    void scheduledToNewDynamicThread(TaskPtr & task);

    void fixed_work(size_t index);
    void dynamic_work(TaskPtr initial_task);

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
