#pragma once

#include <Common/FiberPool.hpp>
#include <Common/ThreadFactory.h>
#include <common/ThreadPool.h>
#include <boost/lockfree/queue.hpp>

namespace DB
{
class IOThreadPool
{
    class ITask
    {
    public:
        ITask() = default;

        virtual ~ITask() = default;
        ITask(const ITask& rhs) = delete;
        ITask& operator=(const ITask& rhs) = delete;
        ITask(ITask&& other) = default;
        ITask& operator=(ITask&& other) = default;

        /**
         * Run the task.
         */
        virtual void execute() = 0;
    };

    template <typename Func>
    class Task: public ITask
    {
    public:
        Task(Func && func_)
            : func{std::move(func_)}
        {}

        ~Task() override = default;
        Task(Task&& other) = default;
        Task& operator=(Task && other) = default;

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

    using Queue = boost::fibers::buffered_channel<std::unique_ptr<ITask>>;
public:
    static IOThreadPool & instance()
    {
        static IOThreadPool pool(400);
        return pool;
    }

    ~IOThreadPool()
    {
        for (auto & queue : queues)
            queue->close();

        for (auto & thread : threads)
            thread.join();
    }

    template <typename Func, typename... Args>
    auto schedule(Func && func, Args &&... args)
    {
        // capature our task into lambda with all its parameters
        auto capture = [func = std::forward<Func>(func),
                        args = std::make_tuple(std::forward<Args>(args)...)]() mutable
            {
                // run the tesk with the parameters provided
                // this will be what our fibers execute
                return std::apply(std::move(func), std::move(args));
            };

        // get return type of our task
        using task_result_t = std::invoke_result_t<decltype(capture)>;

        // create fiber package_task
        using packaged_task_t 
            = boost::fibers::packaged_task<task_result_t()>; 

        packaged_task_t task {std::move(capture)};

        using task_t = Task<packaged_task_t>;
        
        // get future for obtaining future result when 
        // the fiber completes
        auto result_future = task.get_future();

        scheduleTask(std::make_unique<task_t>(std::move(task)));

        // return the future to the caller so that 
        // we can get the result when the fiber with our task 
        // completes
        return std::move(result_future);
    }
private:
    explicit IOThreadPool(size_t initial_size)
        : idle_queues(initial_size)
    {
        for (size_t i = 0; i < initial_size; ++i)
            queues.emplace_back(std::make_unique<Queue>(2));

        for (size_t i = 0; i < initial_size; ++i)
            threads.emplace_back(ThreadFactory(true, "IOTP").newThread(&IOThreadPool::work, this, i));
    }

    void scheduleTask(std::unique_ptr<ITask> task)
    {
        Queue * queue = nullptr;
        if (idle_queues.pop(queue))
        {
            queue->push(std::move(task));
        }
        else
        {
            std::thread t([task = std::move(task)] { task->execute(); });
            t.detach();
        }
    }

    void work(size_t index)
    {
        Queue * queue = queues[index].get();
        while (true)
        {
            idle_queues.push(queue);
            std::unique_ptr<ITask> task;
            auto res = queue->pop(task);
            if (res == boost::fibers::channel_op_status::closed)
                break;
            task->execute();
        }
    }

    std::vector<std::thread> threads;
    std::vector<std::unique_ptr<Queue>> queues;
    boost::lockfree::queue<Queue *> idle_queues;
};
} // namespace DB
