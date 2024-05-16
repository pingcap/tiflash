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

#include <Core/Types.h>
#include <Poco/Event.h>
#include <Poco/Timestamp.h>
#include <absl/synchronization/blocking_counter.h>
#include <Storages/KVStore/FFI/JointThreadAllocInfo.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <pcg_random.hpp>
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
    /// Returns false, the next time will be done later.
    using Task = std::function<bool()>;


    class TaskInfo
    {
    public:
        /// Wake up any thread.
        void wake();

        TaskInfo(
            BackgroundProcessingPool & pool_,
            const Task & function_,
            const bool multi_,
            const uint64_t interval_ms_)
            : pool(pool_)
            , function(function_)
            , multi(multi_)
            , interval_milliseconds(interval_ms_)
        {}

    private:
        friend class BackgroundProcessingPool;

        BackgroundProcessingPool & pool;
        Task function;

        /// Read lock is hold when task is executed.
        std::shared_mutex rwlock;
        std::atomic<bool> removed{false};

        // multi=true, can be run by multiple threads concurrently
        // multi=false, only run on one thread
        const bool multi;
        // The number of worker threads is going to execute this task
        size_t concurrent_executors = 0;

        // User defined execution interval
        const uint64_t interval_milliseconds;

        std::multimap<Poco::Timestamp, std::shared_ptr<TaskInfo>>::iterator iterator;
    };

    using TaskHandle = std::shared_ptr<TaskInfo>;


    explicit BackgroundProcessingPool(int size_, std::string thread_prefix_, JointThreadInfoJeallocMapPtr joint_memory_allocation_map_);

    size_t getNumberOfThreads() const { return size; }

    /// task
    /// - A function return bool.
    /// - Returning true mean some useful work was done. In that case, thread will not sleep before next run of this task.
    /// - Returning false, the next time will be done later.
    /// multi
    /// - If multi == false, this task can only be executed by one thread within each scheduled time.
    /// interval_ms
    /// - If interval_ms is zero, this task will be scheduled with `sleep_seconds`.
    /// - If interval_ms is not zero, this task will be scheduled with `interval_ms`.
    TaskHandle addTask(const Task & task, bool multi = true, size_t interval_ms = 0);
    void removeTask(const TaskHandle & task);

    ~BackgroundProcessingPool();

    std::vector<pid_t> getThreadIds();
    void addThreadId(pid_t tid);

private:
    void threadFunction(size_t thread_idx) noexcept;

    TaskHandle tryPopTask(pcg64 & rng) noexcept;

private:
    using Tasks = std::multimap<Poco::Timestamp, TaskHandle>; /// key is desired next time to execute (priority).
    using Threads = std::vector<std::thread>;

    const size_t size;
    const std::string thread_prefix;
    static constexpr double sleep_seconds = 10;
    static constexpr double sleep_seconds_random_part = 1.0;

    Tasks tasks; /// Ordered in priority.
    std::mutex tasks_mutex;

    Threads threads;
    std::vector<pid_t> thread_ids; // Linux Thread ID
    std::mutex thread_ids_mtx;
    absl::BlockingCounter thread_ids_counter;

    std::atomic<bool> shutdown{false};
    std::condition_variable wake_event;

    JointThreadInfoJeallocMapPtr joint_memory_allocation_map;
};

using BackgroundProcessingPoolPtr = std::shared_ptr<BackgroundProcessingPool>;

} // namespace DB
