#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeTaskPool.h>
#include <Storages/Transaction/TMTContext.h>

#include <ext/scope_guard.h>

namespace DB
{

namespace DM
{

DeltaMergeTaskPool::DeltaMergeTaskPool(Context & db_context)
    : global_context(db_context),
      background_pool(db_context.getBackgroundPool()),
      rate_limiter(std::make_shared<RateLimiter>(
          db_context,
          db_context.getSettingsRef().dt_bg_task_rate_limit,
          db_context.getSettingsRef().dt_bg_task_rate_limit * db_context.getSettingsRef().dt_bg_task_burst_rate_limit_ratio,
          db_context.getSettingsRef().dt_bg_task_rate_limit * db_context.getSettingsRef().dt_bg_task_max_balance_ratio)),
      next_store_id{0},
      log(&Logger::get("DeltaMergeTaskPool"))
{
    background_task_handle = background_pool.addTask([this] { return handleBackgroundTask(); });
}

DeltaMergeTaskPool::~DeltaMergeTaskPool()
{
    LOG_INFO(log, "Destroying DeltaMergeTaskPool");
    background_pool.removeTask(background_task_handle);
    background_task_handle = nullptr;
}

DeltaMergeTaskPoolHandle DeltaMergeTaskPool::registerStore()
{
    return next_store_id++;
}

void DeltaMergeTaskPool::addTask(const BackgroundTask & task, const ThreadType & whom)
{
    LOG_DEBUG(log,
              "Database: [" << task.store->getDatabaseName() << "] Table: [" << task.store->getTableName() << "] Segment ["
                            << task.segment->segmentId() << "] task [" << TaskToString(task.type) << "] add to background task pool by ["
                            << DeltaMergeStore::toString(whom) << "]");

    std::scoped_lock lock(mutex);
    low_priority_tasks.push_back(std::make_shared<BackgroundTask>(task));
    GET_METRIC(global_context.getTiFlashMetrics(), tiflash_storage_delta_merge_task_num, type_low_priority_task_num).Increment(1);
    if (task_counts.find(task.handle) == task_counts.end())
        task_counts.emplace(task.handle, 0);
    task_counts[task.handle] += 1;
}

void DeltaMergeTaskPool::removeAllTasksForStore(DeltaMergeTaskPoolHandle handle, const String & database_name, const String & table_name)
{
    TaskSet processing_tasks_to_wait;
    {
        std::scoped_lock lock{mutex};
        auto             is_target_task = [&handle](const BackgroundTaskHandle & task) { return task->handle == handle; };
        high_priority_tasks.erase(std::remove_if(high_priority_tasks.begin(), high_priority_tasks.end(), is_target_task),
                                  high_priority_tasks.end());
        low_priority_tasks.erase(std::remove_if(low_priority_tasks.begin(), low_priority_tasks.end(), is_target_task),
                                 low_priority_tasks.end());
        for (auto & task : processing_tasks)
        {
            if (is_target_task(task))
                processing_tasks_to_wait.insert(task);
        }
        for (auto & task : processing_tasks_to_wait)
            processing_tasks.erase(task);
        task_counts.erase(handle);
        GET_METRIC(global_context.getTiFlashMetrics(), tiflash_storage_delta_merge_task_num, type_high_priority_task_num)
            .Set(high_priority_tasks.size());
        GET_METRIC(global_context.getTiFlashMetrics(), tiflash_storage_delta_merge_task_num, type_low_priority_task_num)
            .Set(low_priority_tasks.size());
    }

    LOG_DEBUG(log,
              "Remove all background tasks for database [" << database_name << "] table [" << table_name << "] wait for "
                                                           << processing_tasks_to_wait.size() << " tasks to finish");
    for (auto & task : processing_tasks_to_wait)
    {
        std::unique_lock lock{task->task_mutex};
        if (task->finished)
            continue;
        task->finished = true;
    }
    LOG_DEBUG(log, "Remove all background tasks for database [" << database_name << "] table [" << table_name << "] complete");
}

bool DeltaMergeTaskPool::handleBackgroundTask()
{
    bool res = false;
    // try handle task from high_priority_queue first
    if (!handling_high_priority_task.exchange(true))
    {
        res = handleTaskImpl(true);
        handling_high_priority_task.exchange(false);
    }
    // if no task from high_priority_queue can be handled, try handle task from low_priority_queue
    if (!res)
        res = handleTaskImpl(false);
    // if no task from low_priority_queue can be handled,
    // return true when low_priority_tasks is not empty(meaning there is other task that can be handled)
    if (!res)
    {
        std::scoped_lock lock{mutex};
        return !low_priority_tasks.empty();
    }
    return res;
}

size_t DeltaMergeTaskPool::getTaskNumForStore(DeltaMergeTaskPoolHandle handle)
{
    std::scoped_lock lock{mutex};
    if (task_counts.find(handle) == task_counts.end())
        return 0;
    return task_counts[handle];
}

bool DeltaMergeTaskPool::handleTaskImpl(bool high_priority)
{
    auto task = nextTask(high_priority);
    if (!task)
        return false;

    // Update GC safe point before background task
    /// Note that `task.dm_context->db_context` will be free after query is finish. We should not use that in background task.
    auto pd_client = global_context.getTMTContext().getPDClient();
    if (!pd_client->isMock())
    {
        auto safe_point = PDClientHelper::getGCSafePointWithRetry(pd_client,
                                                                  /* ignore_cache= */ false,
                                                                  global_context.getSettingsRef().safe_point_update_interval_seconds);

        LOG_DEBUG(log,
                  "Database: [" << task->store->getDatabaseName() << "] Table: [" << task->store->getTableName() << "] Task"
                                << TaskToString(task->type) << " GC safe point: " << safe_point);

        // Foreground task don't get GC safe point from remote, but we better make it as up to date as possible.
        task->store->updateLatestGcSafePoint(safe_point);
        task->dm_context->min_version = safe_point;
    }

    {
        std::scoped_lock task_lock{task->task_mutex};
        if (task->finished)
            return true;
        if (task->store->isShutdown())
            return true;

        auto try_request_balance = [&](DeltaMergeTaskPool::BackgroundTaskHandle & task, bool clear_snapshot) {
            if (task->task_size <= 0)
                return true;
            task->task_size = rate_limiter->request(task->task_size);
            if (task->task_size <= 0)
                return true;
            else
            {
                if (clear_snapshot)
                {
                    task->snapshot      = nullptr;
                    task->next_snapshot = nullptr;
                }
                // if this task is taken from high_priority_queue, we should put this task back to the head
                // otherwise append it to high_priority_queue
                addTaskToHighPriorityQueue(task, /* front */ high_priority);
                return false;
            }
        };

        // `need_create_snapshot` is used to avoid create too many snapshot for tasks which may cause oom
        // for the task from high priority queue, always create snapshot because no other tasks in the queue will be handled
        //   until the first task completed
        // for the task from low priority queue, create snapshot when high_priority_tasks is empty
        bool need_create_snapshot = false;
        if (high_priority)
            need_create_snapshot = true;
        else
        {
            std::scoped_lock lock{mutex};
            need_create_snapshot = high_priority_tasks.empty();
        }

        SegmentPtr left, right;
        ThreadType type = ThreadType::Write;
        bool       is_physical;
        try
        {
            switch (task->type)
            {
            case Split:
                if (!task->snapshot)
                {
                    std::tie(task->snapshot, std::ignore) = task->store->createSegmentSnapshot(*task->dm_context, task->segment, {}, true);
                    if (!task->snapshot)
                    {
                        LOG_DEBUG(log,
                                  "Database [" << task->store->getDatabaseName() << "] Table [" << task->store->getTableName()
                                               << "] give up segment [" << task->segment->segmentId() << "] split");
                        removeTaskFromProcessingQueue(task);
                        return true;
                    }
                    task->task_size = task->snapshot->getBytes();
                }
                std::tie(is_physical, std::ignore) = task->segment->isPhysicalSplit(*task->dm_context, task->snapshot);
                if (is_physical && !try_request_balance(task, !need_create_snapshot))
                    return false;

                std::tie(left, right) = task->store->segmentSplit(*task->dm_context, task->segment, task->snapshot);
                type                  = ThreadType::BG_Split;
                break;
            case Merge:
                if (!task->snapshot || !task->next_snapshot)
                {
                    std::tie(task->snapshot, task->next_snapshot)
                        = task->store->createSegmentSnapshot(*task->dm_context, task->segment, task->next_segment, true);
                    if (!task->snapshot || !task->next_snapshot)
                    {
                        LOG_DEBUG(log,
                                  "Database [" << task->store->getDatabaseName() << "] Table [" << task->store->getTableName()
                                               << "] give up merge segments left [" << task->segment->segmentId() << "], right ["
                                               << task->next_segment->segmentId() << "]");
                        removeTaskFromProcessingQueue(task);
                        return true;
                    }
                    task->task_size = task->snapshot->getBytes() + task->next_snapshot->getBytes();
                }
                if (!try_request_balance(task, !need_create_snapshot))
                    return false;

                task->store->segmentMerge(*task->dm_context, task->segment, task->next_segment, task->snapshot, task->next_snapshot);
                type = ThreadType::BG_Merge;
                break;
            case MergeDelta:
                if (!task->snapshot)
                {
                    std::tie(task->snapshot, std::ignore) = task->store->createSegmentSnapshot(*task->dm_context, task->segment, {}, true);
                    if (!task->snapshot)
                    {
                        LOG_DEBUG(log,
                                  "Database [" << task->store->getDatabaseName() << "] Table [" << task->store->getTableName()
                                               << "] give up merge delta, segment [" << task->segment->segmentId() << "]");
                        removeTaskFromProcessingQueue(task);
                        return true;
                    }
                    task->task_size = task->snapshot->getBytes();
                }
                if (!try_request_balance(task, !need_create_snapshot))
                    return false;

                left = task->store->segmentMergeDelta(*task->dm_context, task->segment, task->snapshot, false);
                type = ThreadType::BG_MergeDelta;
                break;
            case Compact:
                task->segment->compactDelta(*task->dm_context);
                left = task->segment;
                type = ThreadType::BG_Compact;
                break;
            case Flush:
                task->segment->flushCache(*task->dm_context);
                // After flush cache, better place delta index.
                task->segment->placeDeltaIndex(*task->dm_context);
                left = task->segment;
                type = ThreadType::BG_Flush;
                break;
            case PlaceIndex:
                task->segment->placeDeltaIndex(*task->dm_context);
                break;
            default:
                throw Exception("Unsupported task type: " + TaskToString(task->type));
            }
        }
        catch (Exception & e)
        {
            LOG_ERROR(log,
                      "Database: [" << task->store->getDatabaseName() << "] Table: [" << task->store->getTableName() << "] Task "
                                    << TaskToString(task->type) << " on Segment [" << task->segment->segmentId()
                                    << ((bool)task->next_segment ? ("] and [" + DB::toString(task->next_segment->segmentId())) : "")
                                    << "] failed. Error msg: " << e.message());
            e.addMessage("(Error while handling background task of Database: [" + task->store->getDatabaseName() + "] Table: ["
                         + task->store->getTableName() + "])");
            e.rethrow();
        }

        if (left)
            task->store->checkSegmentUpdate(task->dm_context, left, type);
        if (right)
            task->store->checkSegmentUpdate(task->dm_context, right, type);

        task->finished = true;
    }
    removeTaskFromProcessingQueue(task);

    return true;
}

DeltaMergeTaskPool::BackgroundTaskHandle DeltaMergeTaskPool::nextTask(bool high_priority)
{
    std::scoped_lock lock{mutex};
    auto &           tasks = high_priority ? high_priority_tasks : low_priority_tasks;
    if (tasks.empty())
        return {};
    auto task = tasks.front();
    tasks.pop_front();
    processing_tasks.insert(task);
    if (high_priority)
        GET_METRIC(global_context.getTiFlashMetrics(), tiflash_storage_delta_merge_task_num, type_high_priority_task_num).Decrement(1);
    else
        GET_METRIC(global_context.getTiFlashMetrics(), tiflash_storage_delta_merge_task_num, type_low_priority_task_num).Decrement(1);

    LOG_DEBUG(log,
              "Database: [" << task->store->getDatabaseName() << "] Table: [" << task->store->getTableName() << "] Segment ["
                            << task->segment->segmentId() << "] task [" << TaskToString(task->type) << "] pop from background task pool");

    return task;
}

void DeltaMergeTaskPool::addTaskToHighPriorityQueue(BackgroundTaskHandle & task, bool front)
{
    LOG_DEBUG(log,
              "Database: [" << task->store->getDatabaseName() << "] Table: [" << task->store->getTableName() << "] Segment ["
                            << task->segment->segmentId() << "] task [" << TaskToString(task->type) << "] remaining task size ["
                            << task->task_size << "] add back to background task pool high priority " << (front ? "head" : "tail"));
    std::scoped_lock lock{mutex};
    processing_tasks.erase(task);
    GET_METRIC(global_context.getTiFlashMetrics(), tiflash_storage_delta_merge_task_num, type_high_priority_task_num).Increment(1);
    if (front)
        high_priority_tasks.push_front(task);
    else
        high_priority_tasks.push_back(task);
}

void DeltaMergeTaskPool::removeTaskFromProcessingQueue(DeltaMergeTaskPool::BackgroundTaskHandle & task)
{
    std::scoped_lock lock{mutex};
    if (processing_tasks.find(task) != processing_tasks.end())
    {
        processing_tasks.erase(task);
        task_counts[task->handle] -= 1;
        LOG_TRACE(log,
                  "Database: [" << task->store->getDatabaseName() << "] Table: [" << task->store->getTableName() << "] Segment ["
                                << task->segment->segmentId() << "] task [" << TaskToString(task->type)
                                << "] removed from processing task set");
    }
}

} // namespace DM
} // namespace DB
