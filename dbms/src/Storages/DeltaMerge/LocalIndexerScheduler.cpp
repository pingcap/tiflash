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

#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Storages/DeltaMerge/LocalIndexerScheduler.h>
#include <common/logger_useful.h>
#include <fiu.h>

namespace DB::FailPoints
{
extern const char force_local_index_task_memory_limit_exceeded[];
} // namespace DB::FailPoints


namespace DB::DM
{

bool operator==(const LocalIndexerScheduler::FileID & lhs, const LocalIndexerScheduler::FileID & rhs)
{
    if (lhs.index() != rhs.index())
        return false;

    auto index = lhs.index();
    if (index == 0)
    {
        return std::get<LocalIndexerScheduler::DMFileID>(lhs).id == std::get<LocalIndexerScheduler::DMFileID>(rhs).id;
    }
    else if (index == 1)
    {
        return std::get<LocalIndexerScheduler::ColumnFileTinyID>(lhs).id
            == std::get<LocalIndexerScheduler::ColumnFileTinyID>(rhs).id;
    }
    return false;
}

LocalIndexerScheduler::LocalIndexerScheduler(const Options & options)
    : logger(Logger::get())
    , pool(std::make_unique<ThreadPool>(options.pool_size, options.pool_size, options.pool_size + 1))
    , pool_max_memory_limit(options.memory_limit)
    , pool_current_memory(0)
{
    // QueueSize = PoolSize+1, because our scheduler will try to schedule next task
    // right before the current task is finished.

    LOG_INFO(
        logger,
        "Initialized LocalIndexerScheduler, pool_size={}, memory_limit_mb={:.1f}",
        options.pool_size,
        static_cast<double>(options.memory_limit) / 1024 / 1024);

    if (options.auto_start)
        start();
}

LocalIndexerScheduler::~LocalIndexerScheduler()
{
    LOG_INFO(logger, "LocalIndexerScheduler is destroying. Waiting scheduler and tasks to finish...");

    // First quit the scheduler. Don't schedule more tasks.
    is_shutting_down = true;
    {
        std::unique_lock lock(mutex);
        scheduler_need_wakeup = true;
        scheduler_notifier.notify_all();
    }

    if (is_started)
        scheduler_thread.join();

    // Then wait all running tasks to finish.
    pool.reset();

    LOG_INFO(logger, "LocalIndexerScheduler is destroyed");
}

void LocalIndexerScheduler::start()
{
    if (is_started)
        return;

    scheduler_thread = std::thread([this]() { schedulerLoop(); });
    is_started = true;
}

void LocalIndexerScheduler::waitForFinish()
{
    while (true)
    {
        std::unique_lock lock(mutex);
        if (all_tasks_count == 0 && running_tasks_count == 0)
            return;
        on_finish_notifier.wait(lock);
    }
}

std::tuple<bool, String> LocalIndexerScheduler::pushTask(const Task & task)
{
    bool memory_limit_exceed = pool_max_memory_limit > 0 && task.request_memory > pool_max_memory_limit;
    fiu_do_on(FailPoints::force_local_index_task_memory_limit_exceeded, { memory_limit_exceed = true; });

    if (unlikely(memory_limit_exceed))
        return {
            false,
            fmt::format(
                "Requests memory to build local index exceeds limit (request={} limit={})",
                task.request_memory,
                pool_max_memory_limit),
        };

    std::unique_lock lock(mutex);

    const auto internal_task = std::make_shared<InternalTask>(InternalTask{
        .user_task = task,
        .created_at = Stopwatch(),
        .scheduled_at = Stopwatch(), // Not scheduled
    });

    // Whether task is ready is undertermined. It can be changed any time
    // according to current running tasks.
    // The scheduler will find a better place for this task when meeting it.
    ready_tasks[task.keyspace_id][task.table_id].emplace_back(internal_task);
    ++all_tasks_count;

    scheduler_need_wakeup = true;
    scheduler_notifier.notify_all();
    return {true, ""};
}

size_t LocalIndexerScheduler::dropTasks(KeyspaceID keyspace_id, TableID table_id)
{
    size_t dropped_tasks = 0;

    std::unique_lock lock(mutex);
    if (auto it = ready_tasks.find(keyspace_id); it != ready_tasks.end())
    {
        auto & tasks_by_table = it->second;
        if (auto table_it = tasks_by_table.find(table_id); table_it != tasks_by_table.end())
        {
            dropped_tasks += table_it->second.size();
            tasks_by_table.erase(table_it);
        }
        if (tasks_by_table.empty())
            ready_tasks.erase(it);
    }
    for (auto it = unready_tasks.begin(); it != unready_tasks.end();)
    {
        if ((*it)->user_task.keyspace_id == keyspace_id && (*it)->user_task.table_id == table_id)
        {
            it = unready_tasks.erase(it);
            ++dropped_tasks;
        }
        else
        {
            it++;
        }
    }

    LOG_INFO(logger, "Removed {} tasks, keyspace_id={} table_id={}", dropped_tasks, keyspace_id, table_id);

    return dropped_tasks;
}

bool LocalIndexerScheduler::isTaskReady(std::unique_lock<std::mutex> &, const InternalTaskPtr & task)
{
    for (const auto & file_id : task->user_task.file_ids)
    {
        if (adding_index_page_id_set.find(file_id) != adding_index_page_id_set.end())
            return false;
    }
    return true;
}

void LocalIndexerScheduler::taskOnSchedule(std::unique_lock<std::mutex> &, const InternalTaskPtr & task)
{
    for (const auto & file_id : task->user_task.file_ids)
    {
        auto [it, inserted] = adding_index_page_id_set.insert(file_id);
        RUNTIME_CHECK(inserted);
        UNUSED(it);
    }

    LOG_DEBUG( //
        logger,
        "Start LocalIndex task, keyspace_id={} table_id={} file_ids={} "
        "memory_[this/total/limit]_mb={:.1f}/{:.1f}/{:.1f} all_tasks={}",
        task->user_task.keyspace_id,
        task->user_task.table_id,
        task->user_task.file_ids,
        static_cast<double>(task->user_task.request_memory) / 1024 / 1024,
        static_cast<double>(pool_current_memory) / 1024 / 1024,
        static_cast<double>(pool_max_memory_limit) / 1024 / 1024,
        all_tasks_count);

    // No need to update unready_tasks here, because we will update unready_tasks
    // when iterating the full list.
}

void LocalIndexerScheduler::taskOnFinish(std::unique_lock<std::mutex> & lock, const InternalTaskPtr & task)
{
    for (const auto & file_id : task->user_task.file_ids)
    {
        auto erased = adding_index_page_id_set.erase(file_id);
        RUNTIME_CHECK(erased == 1, erased);
    }

    moveBackReadyTasks(lock);

    auto elapsed_since_create = task->created_at.elapsedSeconds();
    auto elapsed_since_schedule = task->scheduled_at.elapsedSeconds();

    LOG_DEBUG( //
        logger,
        "Finish LocalIndex task, keyspace_id={} table_id={} file_ids={} "
        "memory_[this/total/limit]_mb={:.1f}/{:.1f}/{:.1f} "
        "[schedule/task]_cost_sec={:.1f}/{:.1f}",
        task->user_task.keyspace_id,
        task->user_task.table_id,
        task->user_task.file_ids,
        static_cast<double>(task->user_task.request_memory) / 1024 / 1024,
        static_cast<double>(pool_current_memory) / 1024 / 1024,
        static_cast<double>(pool_max_memory_limit) / 1024 / 1024,
        elapsed_since_create - elapsed_since_schedule,
        elapsed_since_schedule);
}

void LocalIndexerScheduler::moveBackReadyTasks(std::unique_lock<std::mutex> & lock)
{
    for (auto it = unready_tasks.begin(); it != unready_tasks.end();)
    {
        auto & task = *it;
        if (isTaskReady(lock, task))
        {
            ready_tasks[task->user_task.keyspace_id][task->user_task.table_id].emplace_back(task);
            it = unready_tasks.erase(it);
        }
        else
        {
            it++;
        }
    }
}

bool LocalIndexerScheduler::tryAddTaskToPool(std::unique_lock<std::mutex> & lock, const InternalTaskPtr & task)
{
    // Memory limit reached
    if (pool_max_memory_limit > 0 && pool_current_memory + task->user_task.request_memory > pool_max_memory_limit)
    {
        return false;
    }

    auto real_job = [task, this]() {
        const auto old_thread_name = getThreadName();
        setThreadName("LocalIndexPool");

        SCOPE_EXIT({
            std::unique_lock lock(mutex);
            pool_current_memory -= task->user_task.request_memory;
            running_tasks_count--;
            taskOnFinish(lock, task);
            on_finish_notifier.notify_all();

            scheduler_need_wakeup = true;
            scheduler_notifier.notify_all();
            setThreadName(old_thread_name.c_str());
        });

        task->scheduled_at.start();

        try
        {
            task->user_task.workload();
        }
        catch (...)
        {
            tryLogCurrentException(
                logger,
                fmt::format(
                    "LocalIndexScheduler meet exception when running task: keyspace_id={} table_id={}",
                    task->user_task.keyspace_id,
                    task->user_task.table_id));
        }
    };

    RUNTIME_CHECK(pool);
    if (!pool->trySchedule(real_job))
        // Concurrent task limit reached
        return false;

    ++running_tasks_count;
    pool_current_memory += task->user_task.request_memory;
    taskOnSchedule(lock, task);

    return true;
}

LocalIndexerScheduler::ScheduleResult LocalIndexerScheduler::scheduleNextTask(std::unique_lock<std::mutex> & lock)
{
    if (ready_tasks.empty())
        return ScheduleResult::FAIL_NO_TASK;

    // To be fairly between different keyspaces,
    // find the keyspace ID which is just > last_schedule_keyspace_id.
    auto keyspace_it = ready_tasks.upper_bound(last_schedule_keyspace_id);
    if (keyspace_it == ready_tasks.end())
        keyspace_it = ready_tasks.begin();
    const KeyspaceID keyspace_id = keyspace_it->first;

    auto & tasks_by_table = keyspace_it->second;
    RUNTIME_CHECK(!tasks_by_table.empty());

    TableID last_schedule_table_id = InvalidTableID;
    if (last_schedule_table_id_by_ks.find(keyspace_id) != last_schedule_table_id_by_ks.end())
        last_schedule_table_id = last_schedule_table_id_by_ks[keyspace_id];

    // Try to finish all tasks in the last table before moving to the next table.
    auto table_it = tasks_by_table.lower_bound(last_schedule_table_id);
    if (table_it == tasks_by_table.end())
        table_it = tasks_by_table.begin();
    const TableID table_id = table_it->first;

    auto & tasks = table_it->second;
    RUNTIME_CHECK(!tasks.empty());
    auto task_it = tasks.begin();
    auto task = *task_it;

    auto remove_current_task = [&]() {
        tasks.erase(task_it);
        if (tasks.empty())
        {
            tasks_by_table.erase(table_it);
            if (tasks_by_table.empty())
            {
                ready_tasks.erase(keyspace_id);
                last_schedule_table_id_by_ks.erase(keyspace_id);
            }
        }
    };

    if (!isTaskReady(lock, task))
    {
        // The task is not ready. Move it to unready_tasks.
        unready_tasks.emplace_back(task);
        remove_current_task();

        LOG_DEBUG(
            logger,
            "LocalIndex task is not ready, will try again later when it is ready. "
            "keyspace_id={} table_id={} file_ids={}",
            task->user_task.keyspace_id,
            task->user_task.table_id,
            task->user_task.file_ids);

        // Let the caller retry. At next retry, we will continue using this
        // Keyspace+Table and try next task.
        return ScheduleResult::RETRY;
    }

    if (!tryAddTaskToPool(lock, task))
        // The pool is full. May be memory limit reached or concurrent task limit reached.
        // We will not try any more tasks.
        // At next retry, we will continue using this Keyspace+Table and try next task.
        return ScheduleResult::FAIL_FULL;

    last_schedule_table_id_by_ks[keyspace_id] = table_id;
    last_schedule_keyspace_id = keyspace_id;
    remove_current_task();
    all_tasks_count--;

    return ScheduleResult::OK;
}

void LocalIndexerScheduler::schedulerLoop()
{
    setThreadName("LocalIndexSched");

    while (true)
    {
        if (is_shutting_down)
            return;

        std::unique_lock lock(mutex);
        scheduler_notifier.wait(lock, [&] { return scheduler_need_wakeup || is_shutting_down; });
        scheduler_need_wakeup = false;

        try
        {
            while (true)
            {
                if (is_shutting_down)
                    return;

                auto result = scheduleNextTask(lock);
                if (result == ScheduleResult::FAIL_FULL)
                {
                    // Cannot schedule task any more, start to wait
                    break;
                }
                else if (result == ScheduleResult::FAIL_NO_TASK)
                {
                    // No task to schedule, start to wait
                    break;
                }
                else if (result == ScheduleResult::RETRY)
                {
                    // Retry schedule again
                }
                else if (result == ScheduleResult::OK)
                {
                    // Task is scheduled, continue to schedule next task
                }
            }
        }
        catch (...)
        {
            // Catch all exceptions to avoid the scheduler thread to be terminated.
            // We should log the exception here.
            tryLogCurrentException(logger, __PRETTY_FUNCTION__);
        }
    }
}

} // namespace DB::DM
