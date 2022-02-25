#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MinTSOScheduler.h>

namespace DB
{
MinTSOScheduler::MinTSOScheduler(MPPTaskManagerPtr task_manager_, UInt64 soft_limit, UInt64 hard_limit)
    : task_manager(task_manager_)
    , min_tso(0)
    , thread_soft_limit(soft_limit)
    , thread_hard_limit(hard_limit)
    , used_threads(0)
    , log(&Poco::Logger::get("MinTSOScheduler"))
{
    assert(thread_hard_limit >= thread_soft_limit);
}

bool MinTSOScheduler::putWaitingQuery(MPPTaskPtr task)
{
    /// check whether this schedule is disabled or not
    if (thread_hard_limit == 0)
    {
        LOG_FMT_INFO(log, "minTSO schedule is disabled!");
        return false;
    }
    auto id = task->getId();
    std::lock_guard<std::mutex> lock(mu);
    if (min_tso == 0 || id.start_ts <= min_tso) /// must executing
    {
        if (used_threads + task->getNeededThreads() <= thread_hard_limit) /// have threads under thread_hard_limit
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
            task->scheduleThisTask();
            query_task_set->used_threads += task->getNeededThreads();
            ++query_task_set->scheduled_task;
            used_threads += task->getNeededThreads();
            LOG_FMT_INFO(log, "{} becomes the min_tso query and is directly scheduled (active set size = {}) when putting it, after apply for {} threads, used {} of the thread hard limit {}.", id.toString(), active_set.size(), task->getNeededThreads(), used_threads, thread_hard_limit);
        }
        else
        {
            throw Exception(fmt::format("threads are unavailable for the min_tso query {}, need {}, but used {} of the thread hard limit {}, active set size = {}, waiting set size = {}.", id.toString(), task->getNeededThreads(), used_threads, thread_hard_limit, active_set.size(), waiting_set.size()));
        }
    }
    else
    {
        if (used_threads + task->getNeededThreads() <= thread_soft_limit) /// have threads under thread_soft_limit
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
            task->scheduleThisTask();
            query_task_set->used_threads += task->getNeededThreads();
            ++query_task_set->scheduled_task;
            used_threads += task->getNeededThreads();
            LOG_FMT_INFO(log, "{} is directly scheduled (active set size = {}) when putting it due to available threads, after apply for {} threads, used {} of the thread soft limit {}.", id.toString(), active_set.size(), task->getNeededThreads(), used_threads, thread_soft_limit);
        }
        else
        {
            auto query_task_set = task_manager->getQueryTaskSetWithLock(id.start_ts);
            if (nullptr == query_task_set || query_task_set->to_be_cancelled)
            {
                LOG_FMT_INFO(log, "{} is not scheduled when putting it due to miss or being cancelled.", id.toString());
                return false;
            }
            waiting_set.insert(id.start_ts);
            LOG_FMT_INFO(log, "{} is put into waiting set (size={}) due to unavailable threads, apply for {} threads, but used {} of the thread soft limit {}.", id.toString(), waiting_set.size(), task->getNeededThreads(), used_threads, thread_soft_limit);
            return true;
        }
    }
    return false;
}

/// NOTE: call deleteAndScheduleQueries under the lock protection of MPPTaskManager,
/// so this func is called exactly once for a query.
void MinTSOScheduler::deleteAndScheduleQueries(UInt64 query_id)
{
    if (thread_hard_limit == 0) /// check whether this schedule is disabled or not
    {
        LOG_FMT_INFO(log, "minTSO schedule is disabled!");
        return;
    }
    std::lock_guard<std::mutex> lock(mu);

    if (active_set.find(query_id) == active_set.end() && waiting_set.find(query_id) == waiting_set.end()) /// already deleted
    {
        LOG_FMT_INFO(log, "query {} is already deleted!", query_id);
        return;
    }
    /// delete from working set and return threads
    active_set.erase(query_id);
    waiting_set.erase(query_id);
    LOG_FMT_INFO(log, "query {} is deleted from active set {} left {} or waiting set {} left {}.", query_id, active_set.find(query_id) != active_set.end(), active_set.size(), waiting_set.find(query_id) != waiting_set.end(), waiting_set.size());

    auto query_task_set = task_manager->getQueryTaskSetWithoutLock(query_id);
    if (nullptr != query_task_set)
    {
        used_threads -= query_task_set->used_threads;
        query_task_set->used_threads = 0;
        query_task_set->scheduled_task = 0;
    }
    /// update min tso from active_set
    min_tso = query_id == min_tso ? 0 : min_tso;
    if (min_tso == 0 && !active_set.empty())
    {
        min_tso = *active_set.begin();
        LOG_FMT_INFO(log, "min_tso query is updated from {} to {} in active set.", query_id, min_tso);
    }

    /// schedule new tasks
    while (!waiting_set.empty() && used_threads < thread_soft_limit)
    {
        /// find a normal query
        auto current_query_id = *waiting_set.begin();
        query_task_set = task_manager->getQueryTaskSetWithoutLock(current_query_id);
        while (nullptr == query_task_set || query_task_set->to_be_cancelled)
        {
            LOG_FMT_INFO(log, "query {} is removed from waiting set due to miss or being cancelled, left {}.", current_query_id, waiting_set.size());
            waiting_set.erase(current_query_id);
            if (waiting_set.empty())
            {
                LOG_FMT_INFO(log, "waiting_set is empty so return.");
                return;
            }
            current_query_id = *waiting_set.begin();
            query_task_set = task_manager->getQueryTaskSetWithoutLock(current_query_id);
        }
        /// get a snapshot tasks to schedule
        auto to_schedule_tasks = query_task_set->task_map.size() - query_task_set->scheduled_task;
        assert(to_schedule_tasks > 0);
        auto needed_threads = 0;
        for (const auto & task_it : query_task_set->task_map)
        {
            if (!task_it.second->isScheduled())
                needed_threads += task_it.second->getNeededThreads();
        }
        if (used_threads + needed_threads <= thread_soft_limit || ((min_tso == current_query_id || min_tso == 0) && used_threads + needed_threads <= thread_hard_limit))
        {
            query_task_set->scheduled_task += to_schedule_tasks;
            query_task_set->used_threads += needed_threads;
            used_threads += needed_threads;
            active_set.insert(current_query_id);
            waiting_set.erase(current_query_id); /// all already tasks of this query are fully active
            if (min_tso == 0)
            {
                min_tso = current_query_id;
            }
            LOG_FMT_INFO(log, "{} tasks of {} query are scheduled (active set size = {}) due to available threads, after applied for total {} threads, used {} of the thread soft limit {} or the hard limit {} if min_tso query {}.", to_schedule_tasks, current_query_id, active_set.size(), needed_threads, used_threads, thread_soft_limit, thread_hard_limit, min_tso == current_query_id);
            for (const auto & task_it : query_task_set->task_map)
            {
                if (!task_it.second->isScheduled())
                {
                    LOG_FMT_INFO(log, "{} is scheduled due to available threads after applying for {} threads.", task_it.first.toString(), task_it.second->getNeededThreads());
                    task_it.second->scheduleThisTask();
                }
            }
        }
        else
        {
            if (min_tso == current_query_id || min_tso == 0) /// the min_tso query should fully run
            {
                throw Exception(fmt::format("threads are unavailable for the min_tso query {}, need {}, but used {} of the thread hard limit {}, active set size = {}, waiting set size = {}.", min_tso, needed_threads, used_threads, thread_hard_limit, active_set.size(), waiting_set.size()));
            }
            LOG_FMT_INFO(log, "threads are unavailable for the query {}, need {}, but used {} of the thread soft limit {}, active set size = {}, waiting set size = {}", current_query_id, needed_threads, used_threads, thread_soft_limit, active_set.size(), waiting_set.size());
            return;
        }
    }
}

} // namespace DB
