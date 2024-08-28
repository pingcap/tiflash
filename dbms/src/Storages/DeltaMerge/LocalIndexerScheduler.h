// Copyright 2024 PingCAP, Inc.
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

#include <Common/Stopwatch.h>
#include <Common/UniThreadPool.h>
#include <Storages/DeltaMerge/LocalIndexerScheduler_fwd.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/PageDefinesBase.h>

#include <boost/functional/hash.hpp>
#include <condition_variable>

namespace DB::DM
{

// Note: this scheduler is global in the TiFlash instance.
class LocalIndexerScheduler
{
public:
    struct Task
    {
        // Note: The scheduler will try to schedule farely according to keyspace_id and table_id.
        KeyspaceID keyspace_id;
        TableID table_id;

        // Used for the scheduler to avoid concurrently adding index for the same Page (ColumnFileTiny/DMFile).
        std::vector<PageIdU64> page_ids;

        // Used for the scheduler to control the maximum requested memory usage.
        size_t request_memory;

        // The actual index setup workload.
        // The scheduler does not care about the workload.
        ThreadPool::Job workload;
    };

    struct Options
    {
        size_t pool_size = 1;
        size_t memory_limit = 0; // 0 = unlimited
        bool auto_start = true;
    };

private:
    struct InternalTask
    {
        const Task user_task;
        Stopwatch created_at{};
        Stopwatch scheduled_at{};
    };

    using InternalTaskPtr = std::shared_ptr<InternalTask>;

public:
    static LocalIndexerSchedulerPtr create(const Options & options)
    {
        return std::make_shared<LocalIndexerScheduler>(options);
    }

    explicit LocalIndexerScheduler(const Options & options);

    ~LocalIndexerScheduler();

    /**
     * @brief Start the scheduler. In some tests we need to start scheduler
     * after some tasks are pushed.
     */
    void start();

    /**
     * @brief Blocks until there is no tasks remaining in the queue and there is no running tasks.
     * Should be only used in tests.
     */
    void waitForFinish();

    /**
     * @brief Push a task to the pool. The task may not be scheduled immediately.
     */
    void pushTask(const Task & task);

    /**
    * @brief Drop all tasks matching specified keyspace id and table id.
    */
    size_t dropTasks(KeyspaceID keyspace_id, TableID table_id);

private:
    // The set of DMFiles that are currently adding index.
    // There maybe multiple threads trying to add index for the same Page. For example,
    // after logical split two segments share the same DMFile, so that adding index for the two segments
    // could result in adding the same index for the same DMFile. It's just a waste of resource.
    std::unordered_set<PageIdU64> adding_index_page_id_set;

    bool isTaskReady(std::unique_lock<std::mutex> &, const InternalTaskPtr & task);

    void taskOnSchedule(std::unique_lock<std::mutex> &, const InternalTaskPtr & task);

    void taskOnFinish(std::unique_lock<std::mutex> & lock, const InternalTaskPtr & task);

    void moveBackReadyTasks(std::unique_lock<std::mutex> & lock);

private:
    bool is_started = false;
    std::thread scheduler_thread;

    /// Try to add a task to the pool. Returns false if the pool is full
    /// (for example, reaches concurrent task limit or memory limit).
    /// When pool is full, we will not try to schedule any more tasks at this moment.
    ///
    /// Actually there could be possibly small tasks to schedule when
    /// reaching memory limit, but this will cause the scheduler tend to
    /// only schedule small tasks, keep large tasks starving under
    /// heavy pressure.
    bool tryAddTaskToPool(std::unique_lock<std::mutex> & lock, const InternalTaskPtr & task);

    KeyspaceID last_schedule_keyspace_id = 0;
    std::map<KeyspaceID, TableID> last_schedule_table_id_by_ks;

    enum class ScheduleResult
    {
        RETRY,
        FAIL_FULL,
        FAIL_NO_TASK,
        OK,
    };

    ScheduleResult scheduleNextTask(std::unique_lock<std::mutex> & lock);

    void schedulerLoop();

private:
    std::mutex mutex;

    const LoggerPtr logger;

    /// The thread pool for creating indices in the background.
    std::unique_ptr<ThreadPool> pool;
    /// The current memory usage of the pool. It is not accurate and the memory
    /// is determined when task is adding to the pool.
    const size_t pool_max_memory_limit;
    size_t pool_current_memory = 0;

    size_t all_tasks_count = 0; // ready_tasks + unready_tasks
    /// Schedule farely according to keyspace_id, and then according to table_id.
    std::map<KeyspaceID, std::map<TableID, std::list<InternalTaskPtr>>> ready_tasks{};
    /// When the scheduler will stop waiting and try to schedule again?
    /// 1. When a new task is added (and pool is not full)
    /// 2. When a pool task is finished
    std::condition_variable scheduler_notifier;
    bool scheduler_need_wakeup = false; // Avoid false wake-ups.

    /// Notified when one task is finished.
    std::condition_variable on_finish_notifier;
    size_t running_tasks_count = 0;

    /// Some tasks cannot be scheduled at this moment. For example, its DMFile
    /// is used in another index building task. These tasks are extracted
    /// from ready_tasks and put into unready_tasks.
    std::list<InternalTaskPtr> unready_tasks{};

    std::atomic<bool> is_shutting_down = false;
};

} // namespace DB::DM
