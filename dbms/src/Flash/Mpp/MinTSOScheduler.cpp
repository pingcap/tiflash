#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MinTSOScheduler.h>

namespace DB
{
constexpr UInt64 MAX_UINT64 = std::numeric_limits<UInt64>::max();

MinTSOScheduler::MinTSOScheduler(UInt64 soft_limit, UInt64 hard_limit)
    : min_tso(MAX_UINT64)
    , thread_soft_limit(soft_limit)
    , thread_hard_limit(hard_limit)
    , used_threads(0)
    , log(&Poco::Logger::get("MinTSOScheduler"))
{
    auto cores = getNumberOfPhysicalCPUCores();
    active_set_soft_limit = (cores + 2) / 2; /// at least 1
    if (thread_hard_limit == 0 && thread_soft_limit == 0)
    {
        LOG_FMT_INFO(log, "MinTSOScheduler is disabled!");
    }
    else if (thread_hard_limit <= thread_soft_limit)
    {
        LOG_FMT_INFO(log, "thread_hard_limit {} should be larger than thread_soft_limit {}, so MinTSOScheduler set them as {}, {} by default, and active_set_soft_limit is {}.", thread_hard_limit, thread_soft_limit, cores * 1000, cores * 200, active_set_soft_limit);
        thread_hard_limit = cores * 1000; /// generally max_threads == cores, so we support a query runs at most 1000 tasks simultaneously on a node.
        thread_soft_limit = cores * 200;
    }
    else
    {
        LOG_FMT_INFO(log, "thread_hard_limit is {}, thread_soft_limit is {}, and active_set_soft_limit is {} in MinTSOScheduler.", thread_hard_limit, thread_soft_limit, active_set_soft_limit);
    }
}

bool MinTSOScheduler::tryToSchedule(MPPTaskPtr task, MPPTaskManager & task_manager)
{
    /// check whether this schedule is disabled or not
    if (thread_hard_limit == 0 && thread_soft_limit == 0)
    {
        return true;
    }
    const auto & id = task->getId();
    auto query_task_set = task_manager.getQueryTaskSetWithoutLock(id.start_ts);
    if (nullptr == query_task_set || query_task_set->to_be_cancelled)
    {
        LOG_FMT_WARNING(log, "{} is scheduled with miss or cancellation.", id.toString());
        return true;
    }
    return scheduleImp(id.start_ts, query_task_set, task, false);
}

void MinTSOScheduler::deleteCancelledQuery(UInt64 tso, MPPTaskManager & task_manager)
{
    active_set.erase(tso);
    waiting_set.erase(tso);
    auto query_task_set = task_manager.getQueryTaskSetWithoutLock(tso);
    if (query_task_set) /// release all waiting tasks
    {
        while (!query_task_set->waiting_tasks.empty())
        {
            query_task_set->waiting_tasks.front()->scheduleThisTask();
            query_task_set->waiting_tasks.pop();
        }
    }

    /// NOTE: if updated from the waiting_set, the min_tso would hang once the killed query is not fully terminated for a long time.
    updateMinTSO(tso, false, "when cancelling it.");
}

void MinTSOScheduler::deleteThenSchedule(UInt64 tso, MPPTaskManager & task_manager)
{
    if (thread_hard_limit == 0 && thread_soft_limit == 0)
    {
        return;
    }
    auto query_task_set = task_manager.getQueryTaskSetWithoutLock(tso);
    /// return back threads
    if (query_task_set)
    {
        used_threads -= query_task_set->used_threads;
        query_task_set->used_threads = 0;
        query_task_set->scheduled_task = 0;
    }
    LOG_FMT_INFO(log, "query {} (is min = {}) is deleted from active set {} left {} or waiting set {} left {}.", tso, tso == min_tso, active_set.find(tso) != active_set.end(), active_set.size(), waiting_set.find(tso) != waiting_set.end(), waiting_set.size());
    /// delete from working set and return threads for finished or cancelled queries
    active_set.erase(tso);
    waiting_set.erase(tso);
    updateMinTSO(tso, false, "as deleting it.");

    /// as deleted query release some threads, so some tasks would get scheduled.
    scheduleWaitingQueries(task_manager);
}

void MinTSOScheduler::scheduleWaitingQueries(MPPTaskManager & task_manager)
{
    /// schedule new tasks
    while (!waiting_set.empty())
    {
        auto current_query_id = *waiting_set.begin();
        auto query_task_set = task_manager.getQueryTaskSetWithoutLock(current_query_id);
        if (nullptr == query_task_set) /// silently solve this rare case
        {
            LOG_FMT_ERROR(log, "the waiting query {} is not in the task manager.", current_query_id);
            updateMinTSO(current_query_id, false, "as it is not in the task manager.");
            active_set.erase(current_query_id);
            waiting_set.erase(current_query_id);
            continue;
        }

        LOG_FMT_DEBUG(log, "query {} (is min = {}) with {} tasks is to be scheduled from waiting set (size = {}).", current_query_id, current_query_id == min_tso, query_task_set->waiting_tasks.size(), waiting_set.size());
        /// schedule tasks one by one
        while (!query_task_set->waiting_tasks.empty())
        {
            auto task = query_task_set->waiting_tasks.front();
            if (!scheduleImp(current_query_id, query_task_set, task, true))
                return;
            query_task_set->waiting_tasks.pop();
        }
        LOG_FMT_DEBUG(log, "query {} (is min = {}) if scheduled from waiting set (size = {}).", current_query_id, current_query_id == min_tso, waiting_set.size());
        waiting_set.erase(current_query_id); /// all waiting tasks of this query are fully active
    }
}

/// [directly schedule, from waiting set] * [is min_tso query, not] * [can schedule, can't] totally 8 cases.
bool MinTSOScheduler::scheduleImp(UInt64 tso, MPPQueryTaskSetPtr query_task_set, MPPTaskPtr task, bool isWaiting)
{
    auto needed_threads = task->getNeededThreads();
    if ((tso <= min_tso && used_threads + needed_threads <= thread_hard_limit) || ((active_set.size() < active_set_soft_limit || tso <= *active_set.rbegin()) && (used_threads + needed_threads <= thread_soft_limit)))
    {
        updateMinTSO(tso, true, isWaiting ? "from the waiting set" : "when directly schedule it");
        active_set.insert(tso);
        ++query_task_set->scheduled_task;
        query_task_set->used_threads += needed_threads;
        used_threads += needed_threads;
        if (isWaiting)
            task->scheduleThisTask();
        LOG_FMT_INFO(log, "{} is scheduled (active set size = {}) due to available threads {}, after applied for {} threads, used {} of the thread {} limit {}.", task->getId().toString(), active_set.size(), isWaiting ? " from the waiting set" : " directly", needed_threads, used_threads, min_tso == tso ? "hard" : "soft", min_tso == tso ? thread_hard_limit : thread_soft_limit);
        return true;
    }
    else
    {
        if (tso <= min_tso) /// the min_tso query should fully run
        {
            auto msg = fmt::format("threads are unavailable for the min_tso query {} {}, need {}, but used {} of the thread hard limit {}, active set size = {}, waiting set size = {}.", min_tso, isWaiting ? "from the waiting set" : "when directly schedule it", needed_threads, used_threads, thread_hard_limit, active_set.size(), waiting_set.size());
            LOG_FMT_ERROR(log, "{}", msg);
            throw Exception(msg);
        }
        if (!isWaiting)
        {
            waiting_set.insert(tso);
            query_task_set->waiting_tasks.push(task);
        }
        LOG_FMT_INFO(log, "threads are unavailable for the query {} or active set is full (size =  {}), need {}, but used {} of the thread soft limit {},{} waiting set size = {}", tso, active_set.size(), needed_threads, used_threads, thread_soft_limit, isWaiting ? "" : " put into", waiting_set.size());
        return false;
    }
}

void MinTSOScheduler::updateMinTSO(UInt64 tso, bool valid, String msg)
{
    auto old_min_tso = min_tso;
    if (valid)
    {
        min_tso = tso < min_tso ? tso : min_tso;
    }
    else if (tso == min_tso)
    {
        min_tso = active_set.empty() ? MAX_UINT64 : *active_set.begin();
        if (!waiting_set.empty() && *waiting_set.begin() < min_tso)
        {
            min_tso = *waiting_set.begin();
        }
    }
    if (min_tso != old_min_tso) /// if min_tso == MAX_UINT64 and the query tso is not to be cancelled, the used_threads, active_set.size() and waiting_set.size() must be 0.
        LOG_FMT_INFO(log, "min_tso query is updated from {} to {} {}, used threads = {}, active set size = {}, waiting set size = {}.", old_min_tso, min_tso, msg, used_threads, active_set.size(), waiting_set.size());
}
} // namespace DB
