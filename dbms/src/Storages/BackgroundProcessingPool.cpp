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

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/Timespan.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>
#include <pcg_random.hpp>
#include <random>

#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
inline static pid_t getTid()
{
    return syscall(SYS_gettid);
}
#else
inline static pid_t getTid()
{
    return -1;
}
#endif

namespace CurrentMetrics
{
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
        std::unique_lock lock(pool.tasks_mutex);

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


BackgroundProcessingPool::BackgroundProcessingPool(
    int size_,
    std::string thread_prefix_,
    JointThreadInfoJeallocMapPtr joint_memory_allocation_map_)
    : size(size_)
    , thread_prefix(thread_prefix_)
    , thread_ids_counter(size_)
    , joint_memory_allocation_map(joint_memory_allocation_map_)
{
    LOG_INFO(Logger::get(), "Create BackgroundProcessingPool, prefix={} n_threads={}", thread_prefix, size);

    threads.resize(size);
    for (size_t i = 0; i < size; ++i)
    {
        threads[i] = std::thread([this, i] { threadFunction(i); });
    }
}


BackgroundProcessingPool::TaskHandle BackgroundProcessingPool::addTask(
    const Task & task,
    const bool multi,
    const size_t interval_ms)
{
    TaskHandle res = std::make_shared<TaskInfo>(*this, task, multi, interval_ms);

    Poco::Timestamp current_time;

    {
        std::unique_lock lock(tasks_mutex);
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
        std::unique_lock lock(tasks_mutex);
        tasks.erase(task->iterator);
    }
}

BackgroundProcessingPool::~BackgroundProcessingPool()
{
    try
    {
        {
            std::unique_lock lock(tasks_mutex);
            shutdown = true;
        }
        wake_event.notify_all();
        for (std::thread & thread : threads)
            thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

// Try to pop a task from the pool that is ready for execution.
// For task->multi == false, it ensure the task is only pop for one execution threads.
// For task->multi == true, it may pop the task multiple times.
// Return nullptr when the pool is shutting down.
BackgroundProcessingPool::TaskHandle BackgroundProcessingPool::tryPopTask(pcg64 & rng) noexcept
{
    TaskHandle task;
    Poco::Timestamp min_time;

    std::unique_lock lock(tasks_mutex);

    while (!task && !shutdown)
    {
        for (const auto & [task_time, task_handle] : tasks)
        {
            // find the first coming task that no thread is running
            // or can be run by multithreads
            if (!task_handle->removed && (task_handle->concurrent_executors == 0 || task_handle->multi))
            {
                min_time = task_time;
                task = task_handle;
                // add the counter to indicate this task is running by one more thread
                task->concurrent_executors += 1;
                break;
            }
        }

        if (!task)
        {
            /// No tasks ready for execution, wait for a while and check again
            wake_event.wait_for(
                lock,
                std::chrono::duration<double>(
                    sleep_seconds + std::uniform_real_distribution<double>(0, sleep_seconds_random_part)(rng)));
            continue;
        }

        Poco::Timestamp current_time;
        if (min_time > current_time)
        {
            // The coming task is not ready for execution yet, wait for a while
            wake_event.wait_for(
                lock,
                std::chrono::microseconds(
                    min_time - current_time
                    + std::uniform_int_distribution<uint64_t>(0, sleep_seconds_random_part * 1000000)(rng)));
        }
        // here task != nullptr and is ready for execution
        return task;
    }
    return task;
}

void BackgroundProcessingPool::threadFunction(size_t thread_idx) noexcept
{
    const auto name = thread_prefix + std::to_string(thread_idx);
    {
        setThreadName(name.data());
        is_background_thread = true;
        addThreadId(getTid());
        auto ptrs = JointThreadInfoJeallocMap::getPtrs();
        LOG_INFO(DB::Logger::get(), "!!!!!! threadFunction {}", thread_prefix);
        joint_memory_allocation_map->reportThreadAllocInfoForStorage(name, ReportThreadAllocateInfoType::Reset, 0);
        joint_memory_allocation_map->reportThreadAllocInfoForStorage(
            name,
            ReportThreadAllocateInfoType::AllocPtr,
            reinterpret_cast<uint64_t>(std::get<0>(ptrs)));
        joint_memory_allocation_map->reportThreadAllocInfoForStorage(
            name,
            ReportThreadAllocateInfoType::DeallocPtr,
            reinterpret_cast<uint64_t>(std::get<1>(ptrs)));
    }

    SCOPE_EXIT({
        joint_memory_allocation_map->reportThreadAllocInfoForStorage(name, ReportThreadAllocateInfoType::Remove, 0);
    });

    // set up the thread local memory tracker
    auto memory_tracker = MemoryTracker::create(0, root_of_non_query_mem_trackers.get());
    memory_tracker->setMetric(CurrentMetrics::MemoryTrackingInBackgroundProcessingPool);
    current_memory_tracker = memory_tracker.get();

    pcg64 rng(randomSeed());
    std::this_thread::sleep_for(
        std::chrono::duration<double>(std::uniform_real_distribution<double>(0, sleep_seconds_random_part)(rng)));

    while (!shutdown)
    {
        TaskHandle task = tryPopTask(rng);
        if (shutdown)
            break;
        // not shutting down but a null task pop, should not happen
        if (task == nullptr)
        {
            LOG_ERROR(Logger::get(), "a null task has been pop!");
            continue;
        }

        bool done_work = false;
        try
        {
            std::shared_lock<std::shared_mutex> rlock(task->rwlock);
            if (task->removed)
                continue;
            done_work = task->function();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        if (shutdown)
            break;

        // Get the time to sleep before the task in the next run.
        // - If task has done work, it could be executed again immediately.
        // - If not, add delay before next run.
        const auto next_sleep_time_span = [](bool done_work, const TaskHandle & t) {
            if (done_work)
            {
                return Poco::Timespan(0, 0);
            }
            else if (t->interval_milliseconds != 0)
            {
                // Update `next_sleep_time_span` by user-defined interval if the later one is non-zero
                return Poco::Timespan(0, /*microseconds=*/t->interval_milliseconds * 1000);
            }
            else
            {
                // else `sleep_seconds` by default
                return Poco::Timespan(sleep_seconds, 0);
            }
        }(done_work, task);

        {
            std::unique_lock lock(tasks_mutex);

            // the task has been done in this thread
            task->concurrent_executors -= 1;

            if (task->removed)
                continue;

            // reschedule this task
            tasks.erase(task->iterator);
            Poco::Timestamp next_time_to_execute = Poco::Timestamp() + next_sleep_time_span;
            task->iterator = tasks.emplace(next_time_to_execute, task);
        }
    }

    current_memory_tracker = nullptr;
}

std::vector<pid_t> BackgroundProcessingPool::getThreadIds()
{
    thread_ids_counter.Wait();
    std::lock_guard lock(thread_ids_mtx);
    if (thread_ids.size() != size)
    {
        LOG_ERROR(Logger::get(), "thread_ids.size is {}, but {} is required", thread_ids.size(), size);
        throw Exception("Background threads' number not match");
    }
    return thread_ids;
}

void BackgroundProcessingPool::addThreadId(pid_t tid)
{
    {
        std::lock_guard lock(thread_ids_mtx);
        thread_ids.push_back(tid);
    }
    thread_ids_counter.DecrementCount();
}

} // namespace DB
