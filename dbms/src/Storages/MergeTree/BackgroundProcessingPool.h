#pragma once

#include <Core/Types.h>
#include <Poco/Event.h>
#include <Poco/Timestamp.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <thread>

namespace DB
{

/** Using a fixed number of threads, perform an arbitrary number of tasks in an infinite loop.
  * In this case, one task can run simultaneously from different threads.
  * Designed for tasks that perform continuous background work (for example, merge).
  * `Task` is a function that returns a bool - did it do any work.
  * If not, then the next time will be done later.
  */
class BackgroundProcessingPool
{
public:
    /// Returns true, if some useful work was done. In that case, thread will not sleep before next run of this task.
    using Task = std::function<bool()>;


    class TaskInfo
    {
    public:
        /// Wake up any thread.
        void wake();

        TaskInfo(BackgroundProcessingPool & pool_, const Task & function_, const bool multi_, const uint64_t interval_ms_)
            : pool(pool_), function(function_), multi(multi_), interval_millisecond(interval_ms_)
        {}

    private:
        friend class BackgroundProcessingPool;

        BackgroundProcessingPool & pool;
        Task function;

        /// Read lock is hold when task is executed.
        std::shared_mutex rwlock;
        std::atomic<bool> removed{false};

        /// only can be invoked by one thread at same time.
        const bool multi;
        std::atomic_bool occupied{false};

        const uint64_t interval_millisecond;

        std::multimap<Poco::Timestamp, std::shared_ptr<TaskInfo>>::iterator iterator;
    };

    using TaskHandle = std::shared_ptr<TaskInfo>;


    BackgroundProcessingPool(int size_);

    size_t getNumberOfThreads() const { return size; }

    /// If multi == false, this task can only be called by one thread at same time.
    /// If interval_ms is zero, this task will be scheduled with `sleep_seconds`.
    /// If interval_ms is not zero, this task will be scheduled with `interval_ms`.
    TaskHandle addTask(const Task & task, const bool multi = true, const size_t interval_ms = 0);
    void removeTask(const TaskHandle & task);

    ~BackgroundProcessingPool();

private:
    using Tasks = std::multimap<Poco::Timestamp, TaskHandle>; /// key is desired next time to execute (priority).
    using Threads = std::vector<std::thread>;

    const size_t size;
    static constexpr double sleep_seconds = 10;
    static constexpr double sleep_seconds_random_part = 1.0;

    Tasks tasks; /// Ordered in priority.
    std::mutex tasks_mutex;

    Threads threads;

    std::atomic<bool> shutdown{false};
    std::condition_variable wake_event;


    void threadFunction();
};

using BackgroundProcessingPoolPtr = std::shared_ptr<BackgroundProcessingPool>;

} // namespace DB
