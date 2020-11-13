#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeTaskPool.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace DM
{

DeltaMergeTaskPool::DeltaMergeTaskPool(Context & db_context)
    : global_context(db_context),
      background_pool(db_context.getBackgroundPool()),
      rate_limiter(std::make_shared<RateLimiter>(db_context.getSettingsRef().dt_bg_task_max_rate_bytes_balance)),
      log(&Logger::get("DeltaMergeTaskPool"))
{
    LOG_INFO(log, "Creating DeltaMergeTaskPool with rate limit " << db_context.getSettingsRef().dt_bg_task_max_rate_bytes_balance);
    background_task_handle = background_pool.addTask([this] { return handleBackgroundTask(); });
}

DeltaMergeTaskPool::~DeltaMergeTaskPool()
{
    LOG_INFO(log, "Destroying DeltaMergeTaskPool");
    background_pool.removeTask(background_task_handle);
    background_task_handle = nullptr;
}

void DeltaMergeTaskPool::addTask(const BackgroundTask & task, const ThreadType & whom)
{
    LOG_DEBUG(log,
              "Database: [" << task.store->getDatabaseName() << "] Table: [" << task.store->getTableName() << "] Segment ["
                            << task.segment->segmentId() << "] task [" << toString(task.type) << "] add to background task pool by ["
                            << DeltaMergeStore::toString(whom) << "]");

    std::scoped_lock lock(mutex);
    low_priority_tasks.push_back(std::make_shared<BackgroundTask>(task));
    if (task_counts.find(task.store) == task_counts.end())
        task_counts.emplace(task.store, 0);
    task_counts[task.store] += 1;
}

void DeltaMergeTaskPool::removeAllTasksForStore(DeltaMergeStorePtr store)
{
    TaskSet processing_tasks_to_wait;
    {
        std::scoped_lock lock{mutex};
        auto             is_target_task = [&store](const BackgroundTaskHandle & task) { return task->store == store; };
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
        task_counts.erase(store);
    }

    for (auto & task : processing_tasks_to_wait)
    {
        std::unique_lock lock{task->task_mutex};
        if (task->finished)
            continue;
        task->finished = true;
    }
}

bool DeltaMergeTaskPool::handleBackgroundTask()
{
    // try handle task from high_priority_queue first
    bool res = handleTaskImpl(true);
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

size_t DeltaMergeTaskPool::getTaskNumForStore(DeltaMergeStorePtr store)
{
    std::scoped_lock lock{mutex};
    if (task_counts.find(store) == task_counts.end())
        return 0;
    return task_counts[store];
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
                                << toString(task->type) << " GC safe point: " << safe_point);

        // Foreground task don't get GC safe point from remote, but we better make it as up to date as possible.
        task->store->updateLatestGcSafePoint(safe_point);
        task->dm_context->min_version = safe_point;
    }

    {
        std::scoped_lock lock{task->task_mutex};
        if (task->finished)
            return true;

        if (!tryPrepareTask(task))
        {
            // if this task is taken from high_priority_queue, we should put this task back to the head
            // otherwise append it to high_priority_queue
            addTaskToHighPriorityQueue(task, /* front */ high_priority);
            return false;
        }

        SegmentPtr left, right;
        ThreadType type = ThreadType::Write;
        try
        {
            switch (task->type)
            {
            case Split:
                if (task->snapshot)
                {
                    std::tie(left, right) = task->store->segmentSplit(*task->dm_context, task->segment, task->snapshot);
                    type                  = ThreadType::BG_Split;
                }
                break;
            case Merge:
                if (task->snapshot && task->next_snapshot)
                {
                    task->store->segmentMerge(*task->dm_context, task->segment, task->next_segment, task->snapshot, task->next_snapshot);
                    type = ThreadType::BG_Merge;
                }
                break;
            case MergeDelta:
                if (task->snapshot)
                {
                    left = task->store->segmentMergeDelta(*task->dm_context, task->segment, task->snapshot, false);
                    type = ThreadType::BG_MergeDelta;
                }
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
                throw Exception("Unsupported task type: " + toString(task->type));
            }
        }
        catch (const Exception & e)
        {
            LOG_ERROR(log,
                      "Database: [" << task->store->getDatabaseName() << "] Table: [" << task->store->getTableName() << "] Task "
                                    << toString(task->type) << " on Segment [" << task->segment->segmentId()
                                    << ((bool)task->next_segment ? ("] and [" + DB::toString(task->next_segment->segmentId())) : "")
                                    << "] failed. Error msg: " << e.message());
            e.rethrow();
        }

        if (left)
            task->store->checkSegmentUpdate(task->dm_context, left, type);
        if (right)
            task->store->checkSegmentUpdate(task->dm_context, right, type);

        task->finished = true;
    }

    {
        std::scoped_lock lock{mutex};
        processing_tasks.erase(task);
        task_counts[task->store] -= 1;
    }

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

    LOG_DEBUG(log,
              "Database: [" << task->store->getDatabaseName() << "] Table: [" << task->store->getTableName() << "] Segment ["
                            << task->segment->segmentId() << "] task [" << toString(task->type) << "] pop from background task pool");

    return task;
}

void DeltaMergeTaskPool::addTaskToHighPriorityQueue(BackgroundTaskHandle & task, bool front)
{
    LOG_DEBUG(log,
              "Database: [" << task->store->getDatabaseName() << "] Table: [" << task->store->getTableName() << "] Segment ["
                            << task->segment->segmentId() << "] task [" << toString(task->type) << "] remaining task size ["
                            << task->task_size << "] add to background task pool " << (front ? "head" : "tail"));
    std::scoped_lock lock{mutex};
    processing_tasks.erase(task);
    if (front)
        high_priority_tasks.push_front(task);
    else
        high_priority_tasks.push_back(task);
}

bool DeltaMergeTaskPool::tryPrepareTask(DeltaMergeTaskPool::BackgroundTaskHandle & task)
{
    auto try_request_balance = [&](DeltaMergeTaskPool::BackgroundTaskHandle & task) {
        if (task->task_size <= 0)
            return true;
        task->task_size = rate_limiter->request(task->task_size);
        if (task->task_size <= 0)
            return true;
        else
            return false;
    };

    // Task that has already been rate limited
    if (task->task_size != -1)
        return try_request_balance(task);

    // Try to process this task for the first time
    bool is_physical;
    switch (task->type)
    {
    case Split:
        std::tie(task->snapshot, std::ignore) = task->store->createSegmentSnapshot(*task->dm_context, task->segment, {}, true);
        if (!task->snapshot)
        {
            LOG_DEBUG(log,
                      "Database [" << task->store->getDatabaseName() << "] Table [" << task->store->getTableName() << "] give up segment ["
                                   << task->segment->segmentId() << "] split");
            return true;
        }
        is_physical = task->segment->isPhysicalSplit(*task->dm_context, task->snapshot);
        if (is_physical)
        {
            task->task_size = task->snapshot->getBytes();
            return try_request_balance(task);
        }
        else
            return true;
    case Merge:
        std::tie(task->snapshot, task->next_snapshot)
            = task->store->createSegmentSnapshot(*task->dm_context, task->segment, task->next_segment, true);
        if (!task->snapshot || !task->next_snapshot)
        {
            LOG_DEBUG(log,
                      "Database [" << task->store->getDatabaseName() << "] Table [" << task->store->getTableName()
                                   << "] give up merge segments left [" << task->segment->segmentId() << "], right ["
                                   << task->next_segment->segmentId() << "]");
            return true;
        }
        task->task_size = task->snapshot->getBytes() + task->next_snapshot->getBytes();
        return try_request_balance(task);
    case MergeDelta:
        std::tie(task->snapshot, std::ignore) = task->store->createSegmentSnapshot(*task->dm_context, task->segment, {}, true);
        if (!task->snapshot)
        {
            LOG_DEBUG(log,
                      "Database [" << task->store->getDatabaseName() << "] Table [" << task->store->getTableName()
                                   << "] give up merge delta, segment [" << task->segment->segmentId() << "]");
            return true;
        }
        task->task_size = task->snapshot->getBytes();
        return try_request_balance(task);
    case Compact:
    case Flush:
    case PlaceIndex:
        return true;
    default:
        throw Exception("Unsupported task type: " + toString(task->type));
    }
}

} // namespace DM
} // namespace DB
