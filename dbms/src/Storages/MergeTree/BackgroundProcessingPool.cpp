#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <IO/WriteHelpers.h>
#include <Poco/Timespan.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <common/logger_useful.h>

#include <pcg_random.hpp>
#include <random>


namespace CurrentMetrics
{
extern const Metric BackgroundPoolTask;
extern const Metric MemoryTrackingInBackgroundProcessingPool;
} // namespace CurrentMetrics

namespace DB
{


constexpr double BackgroundProcessingPool::sleep_seconds;
constexpr double BackgroundProcessingPool::sleep_seconds_random_part;

#if __APPLE__ && __clang__
__thread bool is_background_thread = false;
#else
thread_local bool is_background_thread = false;
#endif

void BackgroundProcessingPool::TaskInfo::wake()
{
    if (removed)
        return;

    Poco::Timestamp current_time;

    {
        std::unique_lock<std::mutex> lock(pool.tasks_mutex);

        auto next_time_to_execute = iterator->first;
        TaskHandle this_task_handle = iterator->second;

        /// If this task was done nothing at previous time and it has to sleep, then cancel sleep time.
        if (next_time_to_execute > current_time)
            next_time_to_execute = current_time;

        pool.tasks.erase(iterator);
        iterator = pool.tasks.emplace(next_time_to_execute, this_task_handle);
    }

    /// Note that if all threads are currently do some work, this call will not wakeup any thread.
    pool.wake_event.notify_one();
}


BackgroundProcessingPool::BackgroundProcessingPool(int size_) : size(size_)
{
    LOG_INFO(&Logger::get("BackgroundProcessingPool"), "Create BackgroundProcessingPool with " << size << " threads");

    threads.resize(size);
    for (auto & thread : threads)
        thread = std::thread([this] { threadFunction(); });
}


BackgroundProcessingPool::TaskHandle BackgroundProcessingPool::addTask(const Task & task, const bool multi, const size_t interval_ms)
{
    TaskHandle res = std::make_shared<TaskInfo>(*this, task, multi, interval_ms);

    Poco::Timestamp current_time;

    {
        std::unique_lock<std::mutex> lock(tasks_mutex);
        res->iterator = tasks.emplace(current_time, res);
    }

    wake_event.notify_all();

    return res;
}

void BackgroundProcessingPool::removeTask(const TaskHandle & task)
{
    if (task->removed.exchange(true))
        return;

    /// Wait for all executions of this task.
    {
        std::unique_lock<std::shared_mutex> wlock(task->rwlock);
    }

    {
        std::unique_lock<std::mutex> lock(tasks_mutex);
        tasks.erase(task->iterator);
    }
}

BackgroundProcessingPool::~BackgroundProcessingPool()
{
    try
    {
        shutdown = true;
        wake_event.notify_all();
        for (std::thread & thread : threads)
            thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void BackgroundProcessingPool::threadFunction()
{
    {
        static std::atomic_uint64_t tid{0};
        const auto name = "BkgPool" + std::to_string(tid++);
        setThreadName(name.data());
        is_background_thread = true;
    }

    MemoryTracker memory_tracker;
    memory_tracker.setMetric(CurrentMetrics::MemoryTrackingInBackgroundProcessingPool);
    current_memory_tracker = &memory_tracker;

    pcg64 rng(randomSeed());
    std::this_thread::sleep_for(std::chrono::duration<double>(std::uniform_real_distribution<double>(0, sleep_seconds_random_part)(rng)));

    while (!shutdown)
    {
        TaskHandle task;
        // The time to sleep before running next task, `sleep_seconds` by default.
        Poco::Timespan next_sleep_time_span(sleep_seconds, 0);

        try
        {
            Poco::Timestamp min_time;

            {
                std::unique_lock<std::mutex> lock(tasks_mutex);

                if (!tasks.empty())
                {
                    for (const auto & time_handle : tasks)
                    {
                        if (!time_handle.second->removed)
                        {
                            min_time = time_handle.first;
                            task = time_handle.second;
                            break;
                        }
                    }
                }
            }

            if (shutdown)
                break;

            if (!task)
            {
                std::unique_lock<std::mutex> lock(tasks_mutex);
                wake_event.wait_for(lock,
                    std::chrono::duration<double>(
                        sleep_seconds + std::uniform_real_distribution<double>(0, sleep_seconds_random_part)(rng)));
                continue;
            }

            /// No tasks ready for execution.
            Poco::Timestamp current_time;
            if (min_time > current_time)
            {
                std::unique_lock<std::mutex> lock(tasks_mutex);
                wake_event.wait_for(lock,
                    std::chrono::microseconds(
                        min_time - current_time + std::uniform_int_distribution<uint64_t>(0, sleep_seconds_random_part * 1000000)(rng)));
            }

            std::shared_lock<std::shared_mutex> rlock(task->rwlock);

            if (task->removed)
                continue;

            {
                CurrentMetrics::Increment metric_increment{CurrentMetrics::BackgroundPoolTask};

                bool done_work = false;
                if (!task->multi)
                {
                    bool expected = false;
                    if (task->occupied == expected && task->occupied.compare_exchange_strong(expected, true))
                    {
                        done_work = task->function();
                        task->occupied = false;
                    }
                    else
                        done_work = false;
                }
                else
                    done_work = task->function();

                /// If task has done work, it could be executed again immediately.
                /// If not, add delay before next run.
                if (done_work)
                {
                    next_sleep_time_span = 0;
                }
                else if (task->interval_millisecond != 0)
                {
                    // Update `next_sleep_time_span` by user-defined interval if the later one is non-zero
                    next_sleep_time_span = Poco::Timespan(0, /*microseconds=*/task->interval_millisecond * 1000);
                }
                // else `sleep_seconds` by default
            }
        }
        catch (...)
        {
            if (task && !task->multi)
            {
                std::unique_lock<std::shared_mutex> wlock(task->rwlock);
                task->occupied = false;
            }

            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        if (shutdown)
            break;

        Poco::Timestamp next_time_to_execute = Poco::Timestamp() + next_sleep_time_span;

        {
            std::unique_lock<std::mutex> lock(tasks_mutex);

            if (task->removed)
                continue;

            tasks.erase(task->iterator);
            task->iterator = tasks.emplace(next_time_to_execute, task);
        }
    }

    current_memory_tracker = nullptr;
}

} // namespace DB
