#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MinTSOScheduler.h>

namespace DB
{
const UInt64 MAX_UINT64 = std::numeric_limits<UInt64>::max();

MinTSOScheduler::MinTSOScheduler(UInt64 soft_limit, UInt64 hard_limit)
    : min_tso(MAX_UINT64)
    , thread_soft_limit(soft_limit)
    , thread_hard_limit(hard_limit)
    , used_threads(0)
    , log(&Poco::Logger::get("MinTSOScheduler"))
{
    assert(thread_hard_limit >= thread_soft_limit);
}

/// try to schedule this task if it is the min_tso query or there are enough threads, otherwise put it into the waiting set.
/// NOTE: call tryToSchedule under the lock protection of MPPTaskManager
bool MinTSOScheduler::tryToSchedule(MPPTaskPtr task, MPPTaskManager & task_manager)
{
    /// check whether this schedule is disabled or not
    if (thread_hard_limit == 0)
    {
        LOG_FMT_INFO(log, "minTSO schedule is disabled!");
        return true;
    }
    auto & id = task->getId();
    auto query_task_set = task_manager.getQueryTaskSetWithoutLock(id.start_ts);
    if (nullptr == query_task_set || query_task_set->to_be_cancelled)
    {
        LOG_FMT_WARNING(log, "{} is scheduled with miss or cancellation.", id.toString());
        return true;
    }
    auto needed_threads = task->getNeededThreads();
    if (id.start_ts <= min_tso) /// must execute
    {
        if (used_threads + needed_threads <= thread_hard_limit) /// have threads under thread_hard_limit
        {
            min_tso = id.start_ts;
            scheduleImp(id.start_ts, query_task_set, needed_threads);
            LOG_FMT_INFO(log, "{} becomes the min_tso query and is directly scheduled (active set size = {}), after apply for {} threads, used {} of the thread hard limit {}.", id.toString(), active_set.size(), needed_threads, used_threads, thread_hard_limit);
        }
        else
        {
            throw Exception(fmt::format("threads are unavailable for the min_tso query {}, need {}, but used {} of the thread hard limit {}, active set size = {}, waiting set size = {}.", id.toString(), needed_threads, used_threads, thread_hard_limit, active_set.size(), waiting_set.size()));
        }
    }
    else
    {
        if ((active_set.size() < 15 || id.start_ts <= *active_set.rbegin()) && used_threads + needed_threads <= thread_soft_limit) /// have threads under thread_soft_limit
        {
            scheduleImp(id.start_ts, query_task_set, needed_threads);
            LOG_FMT_INFO(log, "{} is directly scheduled (active set size = {}) due to available threads, after apply for {} threads, used {} of the thread soft limit {}.", id.toString(), active_set.size(), needed_threads, used_threads, thread_soft_limit);
        }
        else
        {
            waiting_set.insert(id.start_ts);
            query_task_set->waiting_tasks.push(task);
            LOG_FMT_INFO(log, "{} is put into waiting set (size={}) due to unavailable threads or active set is full (size =  {}), apply for {} threads, but used {} of the thread soft limit {}.", id.toString(), waiting_set.size(), active_set.size(), needed_threads, used_threads, thread_soft_limit);
            return false;
        }
    }
    return true;
}

/// delete this to-be cancelled query from scheduler and update min_tso if needed, so that there aren't cancelled queries in the scheduler.
/// NOTE: call deleteQueryIfCancelled under the lock protection of MPPTaskManager
void MinTSOScheduler::deleteQueryIfCancelled(UInt64 query_id, MPPTaskManager & task_manager)
{
    active_set.erase(query_id);
    if (query_id == min_tso)
    {
        min_tso = active_set.empty() ? MAX_UINT64 : *active_set.begin();
    }
    waiting_set.erase(query_id);
    auto query_task_set = task_manager.getQueryTaskSetWithoutLock(query_id);
    if (nullptr != query_task_set) /// release all waiting tasks
    {
        while (!query_task_set->waiting_tasks.empty())
        {
            query_task_set->waiting_tasks.front()->scheduleThisTask();
            query_task_set->waiting_tasks.pop();
        }
    }
}

/// delete the query in the active set and waiting set and release threads, then schedule waiting tasks.
/// NOTE: call deleteAndScheduleQueries under the lock protection of MPPTaskManager,
/// so this func is called exactly once for a query.
void MinTSOScheduler::deleteAndScheduleQueries(UInt64 query_id, MPPTaskManager & task_manager)
{
    if (thread_hard_limit == 0) /// check whether this schedule is disabled or not
    {
        LOG_FMT_INFO(log, "minTSO schedule is disabled!");
        return;
    }
    /// delete from working set and return threads for finished or cancelled queries
    active_set.erase(query_id);
    waiting_set.erase(query_id);
    LOG_FMT_INFO(log, "query {} is deleted from active set {} left {} or waiting set {} left {}.", query_id, active_set.find(query_id) != active_set.end(), active_set.size(), waiting_set.find(query_id) != waiting_set.end(), waiting_set.size());
    auto query_task_set = task_manager.getQueryTaskSetWithoutLock(query_id);
    if (nullptr != query_task_set)
    {
        used_threads -= query_task_set->used_threads;
        query_task_set->used_threads = 0;
        query_task_set->scheduled_task = 0;
    }
    /// update min_tso from active_set
    if (query_id == min_tso)
    {
        min_tso = active_set.empty() ? MAX_UINT64 : *active_set.begin();
        LOG_FMT_INFO(log, "min_tso query is updated from {} to {} in active set.", query_id, min_tso);
    }

    /// schedule new tasks
    while (!waiting_set.empty())
    {
        auto current_query_id = *waiting_set.begin();
        query_task_set = task_manager.getQueryTaskSetWithoutLock(current_query_id);
        if (nullptr == query_task_set)
        {
            min_tso = current_query_id == min_tso ? MAX_UINT64 : min_tso;
            throw Exception(fmt::format("the waiting query {} is not in the task manager.", current_query_id));
        }
        min_tso = current_query_id < min_tso ? current_query_id : min_tso;
        LOG_FMT_INFO(log, "query {} (min_tso = {}) with {} tasks is to be scheduled from waiting set (size = {}).", current_query_id, current_query_id == min_tso, query_task_set->waiting_tasks.size(), waiting_set.size());

        /// schedule tasks one by one
        while (!query_task_set->waiting_tasks.empty())
        {
            auto task = query_task_set->waiting_tasks.front();
            auto needed_threads = task->getNeededThreads();
            if ((active_set.size() < 15 || current_query_id <= *active_set.rbegin()) && (used_threads + needed_threads <= thread_soft_limit || (min_tso == current_query_id && used_threads + needed_threads <= thread_hard_limit)))
            {
                scheduleImp(current_query_id, query_task_set, needed_threads);
                task->scheduleThisTask();
                query_task_set->waiting_tasks.pop();
                LOG_FMT_INFO(log, "{} is scheduled (active set size = {}) due to available threads, after applied for {} threads, used {} of the thread soft limit {} or the hard limit {} by {}.", task->getId().toString(), active_set.size(), needed_threads, used_threads, thread_soft_limit, thread_hard_limit, min_tso == current_query_id);
            }
            else
            {
                if (min_tso == current_query_id) /// the min_tso query should fully run
                {
                    throw Exception(fmt::format("threads are unavailable for the min_tso query {}, need {}, but used {} of the thread hard limit {}, active set size = {}, waiting set size = {}.", min_tso, needed_threads, used_threads, thread_hard_limit, active_set.size(), waiting_set.size()));
                }
                LOG_FMT_INFO(log, "threads are unavailable for the query {} or active set is full (size =  {}), need {}, but used {} of the thread soft limit {}, active set size = {}, waiting set size = {}", current_query_id, active_set.size(), needed_threads, used_threads, thread_soft_limit, active_set.size(), waiting_set.size());
                return;
            }
        }
        LOG_FMT_INFO(log, "query {} (min tso = {}) with {} tasks are scheduled from waiting set (size = {}).", current_query_id, current_query_id == min_tso, query_task_set->waiting_tasks.size(), waiting_set.size());
        waiting_set.erase(current_query_id); /// all waiting tasks of this query are fully active
    }
}

void MinTSOScheduler::scheduleImp(UInt64 tso, MPPQueryTaskSetPtr query_task_set, int needed_threads)
{
    active_set.insert(tso);
    ++query_task_set->scheduled_task;
    query_task_set->used_threads += needed_threads;
    used_threads += needed_threads;
}

} // namespace DB
