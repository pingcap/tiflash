#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MinTSOScheduler.h>

namespace DB
{
MinTSOScheduler::MinTSOScheduler(MPPTaskManagerPtr task_manager_)
    : task_manager(task_manager_)
    , min_tso(0)
    , thread_soft_limit(5000)
    , thread_hard_limit(8000)
    , used_threads(0)
    , default_threads(100)
    , log(&Poco::Logger::get("MinTSOScheduler"))
{
    assert(thread_hard_limit > thread_soft_limit);
}

bool MinTSOScheduler::putWaitingQuery(MPPTaskPtr task)
{
    auto id = task->getId();
    std::lock_guard<std::mutex> lock(mu);
    if (min_tso == 0 || id.start_ts <= min_tso) /// must executing
    {
        if (used_threads + default_threads <= thread_hard_limit) /// have threads under thread_hard_limit
        {
            auto query_task_set = task_manager->getQueryTaskSetWithLock(id.start_ts);

            if (nullptr == query_task_set || query_task_set->to_be_cancelled)
            {
                return false;
            }
            if (query_task_set->scheduled_task == 0 && !query_task_set->to_be_cancelled)
            {
                active_set.insert(id.start_ts);
                min_tso = id.start_ts;
            }
            query_task_set->used_threads += default_threads;
            ++query_task_set->scheduled_task;
            used_threads += default_threads;
        }
        else
        {
            throw Exception("threads are unavailable for the min_tso query!");
        }
    }
    else
    {
        if (used_threads + default_threads <= thread_soft_limit) /// have threads under thread_soft_limit
        {
            auto query_task_set = task_manager->getQueryTaskSetWithLock(id.start_ts);

            if (nullptr == query_task_set || query_task_set->to_be_cancelled)
            {
                return false;
            }
            if (query_task_set->scheduled_task == 0 && !query_task_set->to_be_cancelled)
            {
                active_set.insert(id.start_ts);
            }
            query_task_set->used_threads += default_threads;
            ++query_task_set->scheduled_task;
            used_threads += default_threads;
        }
        else
        {
            auto query_task_set = task_manager->getQueryTaskSetWithLock(id.start_ts);
            if (nullptr == query_task_set || query_task_set->to_be_cancelled)
            {
                return false;
            }
            waiting_set.insert(id.start_ts);
            return true;
        }
    }
    return false;
}

/// NOTE: call deleteAndScheduleQueries under the lock protection of MPPTaskManager,
/// so this func is called exactly once for a query.
void MinTSOScheduler::deleteAndScheduleQueries(UInt64 query_id)
{
    std::lock_guard<std::mutex> lock(mu);
    /// delete from working set and return threads
    active_set.erase(query_id);
    waiting_set.erase(query_id);
    auto query_task_set = task_manager->getQueryTaskSetWithoutLock(query_id);
    if (nullptr != query_task_set)
    {
        used_threads -= query_task_set->used_threads;
        query_task_set->used_threads = 0;
    }
    /// update min tso from active_set
    min_tso = query_id == min_tso ? 0 : min_tso;
    if (min_tso == 0 && !active_set.empty())
    {
        min_tso = *active_set.begin();
    }

    /// schedule new tasks
    while (!waiting_set.empty() && used_threads + default_threads <= thread_soft_limit)
    {
        /// find a normal query
        UInt64 current_query_id = 0;
        query_task_set = task_manager->getQueryTaskSetWithoutLock(current_query_id);
        while (nullptr == query_task_set || query_task_set->to_be_cancelled)
        {
            waiting_set.erase(current_query_id);
            if (waiting_set.empty())
            {
                return;
            }
            current_query_id = *waiting_set.begin();
            query_task_set = task_manager->getQueryTaskSetWithoutLock(current_query_id);
        }
        /// get a snapshot tasks to schedule (TODO: schedule tasks in batch)
        auto to_schedule_tasks = query_task_set->task_map.size() - query_task_set->scheduled_task;
        auto needed_threads = to_schedule_tasks * default_threads;
        if (used_threads + needed_threads <= thread_soft_limit || ((min_tso == current_query_id || min_tso == 0) && used_threads + needed_threads <= thread_hard_limit))
        {
            query_task_set->scheduled_task += to_schedule_tasks;
            query_task_set->used_threads += needed_threads;
            used_threads += needed_threads;
            active_set.insert(current_query_id);
            waiting_set.erase(current_query_id); /// all tasks of this query are fully active
            if (min_tso == 0)
            {
                min_tso = current_query_id;
            }
            for (const auto & task_it : query_task_set->task_map)
            {
                task_it.second->scheduleThisTask();
            }
        }
        else
        {
            if (min_tso == current_query_id || min_tso == 0) /// the min_tso query should fully run
            {
                throw Exception("threads are unavailable for the min_tso query!");
            }
            return;
        }
    }
}
} // namespace DB
