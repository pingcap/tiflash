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
class Context;

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

        /// only can be invoked by one thread at same time.
        const bool multi;
        std::atomic_bool occupied{false};

        const uint64_t interval_milliseconds;

        std::multimap<Poco::Timestamp, std::shared_ptr<TaskInfo>>::iterator iterator;
    };

    using TaskHandle = std::shared_ptr<TaskInfo>;


    explicit BackgroundProcessingPool(int size_, std::string thread_prefix_);

    size_t getNumberOfThreads() const { return size; }

    /// if multi == false, this task can only be called by one thread at same time.
    /// If interval_ms is zero, this task will be scheduled with `sleep_seconds`.
    /// If interval_ms is not zero, this task will be scheduled with `interval_ms`.
    ///
    /// But at each scheduled time, there may be multiple threads try to run the same task,
    ///   and then execute the same task one by one in sequential order(not simultaneously) even if `multi` is false.
    /// For example, consider the following case when it's time to schedule a task,
    /// 1. thread A get the task, mark the task as occupied and begin to execute it
    /// 2. thread B also get the same task
    /// 3. thread A finish the execution of the task quickly, release the task and try to update the next schedule time of the task
    /// 4. thread B find the task is not occupied and execute the task again almost immediately
    TaskHandle addTask(const Task & task, bool multi = true, size_t interval_ms = 0);
    void removeTask(const TaskHandle & task);

    ~BackgroundProcessingPool();

    std::vector<pid_t> getThreadIds();
    void addThreadId(pid_t tid);

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

    void threadFunction(size_t thread_idx);
};

using BackgroundProcessingPoolPtr = std::shared_ptr<BackgroundProcessingPool>;

} // namespace DB
