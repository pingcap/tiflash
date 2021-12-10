#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadFactory.h>

#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <future>

namespace DB
{
class DynamicThreadPool
{
private:
    class ITask
    {
    public:
        ITask() = default;

        virtual ~ITask() = default;
        ITask(const ITask & rhs) = delete;
        ITask & operator=(const ITask & rhs) = delete;
        ITask(ITask && other) = default;
        ITask & operator=(ITask && other) = default;

        /**
         * Run the task.
         */
        virtual void execute() = 0;
    };

    template <typename Func>
    class Task : public ITask
    {
    public:
        Task(Func && func_)
            : func{std::move(func_)}
        {}

        ~Task() override = default;
        Task(Task && other) = default;
        Task & operator=(Task && other) = default;

        /**
         * Run the task.
         */
        void execute() override
        {
            func();
        }

    private:
        Func func;
    };

    using TaskPtr = std::shared_ptr<ITask>;
    using Queue = ConcurrentBoundedQueue<TaskPtr>;

    // used for dynamic threads
    struct DynamicNode
    {
        DynamicNode()
        {
            next = this;
            prev = this;
        }

        void prepend(DynamicNode * head)
        {
            next = head->next;
            prev = head;
            head->next->prev = this;
            head->next = this;
        }

        void detach()
        {
            prev->next = next;
            next->prev = prev;
            next = this;
            prev = this;
        }

        bool empty() const
        {
            return next == prev;
        }

        DynamicNode * next;
        DynamicNode * prev;
        std::condition_variable cv;
        TaskPtr task;
    };

public:
    explicit DynamicThreadPool(size_t initial_size)
        : idle_fixed_queues(initial_size)
    {
        for (size_t i = 0; i < initial_size; ++i)
            fixed_queues.emplace_back(std::make_unique<Queue>(2)); // reserve 2 slots to avoid blocking: one schedule, one dtor.

        for (size_t i = 0; i < initial_size; ++i)
            fixed_threads.emplace_back(ThreadFactory(true, "FixedThread").newThread(&DynamicThreadPool::fixed_work, this, i));
    }

    ~DynamicThreadPool()
    {
        for (auto & queue : fixed_queues)
            queue->push(nullptr);

        for (auto & thread : fixed_threads)
            thread.join();

        {
            std::unique_lock lock(dynamic_mutex);
            in_destructing = true;
            for (auto * next = dynamic_idle_head.next; next != &dynamic_idle_head; next = next->next)
                next->cv.notify_one();
        }

        while (alive_dynamic_threads.load() != 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    template <typename Func, typename... Args>
    auto schedule(Func && func, Args &&... args)
    {
        // capature our task into lambda with all its parameters
        auto capture = [func = std::forward<Func>(func),
                        args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
            // run the task with the parameters provided
            return std::apply(std::move(func), std::move(args));
        };

        // get return type of our task
        using TaskResult = std::invoke_result_t<decltype(capture)>;

        // create package_task to capture exceptions
        using PackagedTask = std::packaged_task<TaskResult()>;
        PackagedTask task{std::move(capture)};

        // get future for obtaining future result
        auto result_future = task.get_future();
        scheduleTask(std::make_unique<Task<PackagedTask>>(std::move(task)));

        return std::move(result_future);
    }

private:
    void scheduleTask(TaskPtr task)
    {
        if (!scheduledToFixedThread(task) && !scheduledToExistedDynamicThread(task))
            scheduledToNewDynamicThread(task);
    }

    bool scheduledToFixedThread(TaskPtr task)
    {
        Queue * queue = nullptr;
        if (idle_fixed_queues.pop(queue))
        {
            queue->push(std::move(task));
            return true;
        }
        return false;
    }

    bool scheduledToExistedDynamicThread(TaskPtr task)
    {
        std::unique_lock lock(dynamic_mutex);
        if (dynamic_idle_head.empty())
            return false;
        DynamicNode * next = dynamic_idle_head.next;
        next->detach();
        next->task = std::move(task);
        next->cv.notify_one();
        return true;
    }

    void scheduledToNewDynamicThread(TaskPtr task)
    {
        alive_dynamic_threads.fetch_add(1);
        std::thread t = ThreadFactory(true, "DynamicThread").newThread(&DynamicThreadPool::dynamic_work, this, std::move(task));
        t.detach();
    }

    void fixed_work(size_t index)
    {
        Queue * queue = fixed_queues[index].get();
        while (true)
        {
            idle_fixed_queues.push(queue);
            TaskPtr task;
            queue->pop(task);
            if (!task)
                break;
            task->execute();
        }
    }

    void dynamic_work(TaskPtr initial_task)
    {
        initial_task->execute();

        static constexpr auto timeout = std::chrono::minutes(10);
        DynamicNode node;
        while (true)
        {
            TaskPtr task;
            {
                std::unique_lock lock(dynamic_mutex);
                if (in_destructing)
                    break;
                node.prepend(&dynamic_idle_head); // prepend to reuse hot threads so that cold threads have chance to exit
                node.cv.wait_for(lock, timeout);
                task = std::move(node.task);
            }

            if (!task) // may be timeout or cancelled
                break;
            task->execute();
        }
        alive_dynamic_threads.fetch_sub(1);
    }

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
