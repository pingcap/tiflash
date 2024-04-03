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

#include <Common/TiFlashMetrics.h>
#include <Common/getNumberOfLogicalCPUCores.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MinTSOScheduler.h>

namespace DB
{
constexpr UInt64 MAX_UINT64 = std::numeric_limits<UInt64>::max();
constexpr UInt64 OS_THREAD_SOFT_LIMIT = 100000;

MinTSOScheduler::MinTSOScheduler(UInt64 soft_limit, UInt64 hard_limit)
    : min_tso(MAX_UINT64)
    , thread_soft_limit(soft_limit)
    , thread_hard_limit(hard_limit)
    , estimated_thread_usage(0)
    , log(&Poco::Logger::get("MinTSOScheduler"))
{
    auto cores = static_cast<size_t>(getNumberOfLogicalCPUCores() / 2);
    if (active_set_soft_limit == 0 || active_set_soft_limit > 100 * cores)
    {
        /// set active_set_soft_limit to a reasonable value
        active_set_soft_limit = (cores + 2) / 2; /// at least 1
    }
    if (isDisabled())
    {
        LOG_FMT_INFO(log, "MinTSOScheduler is disabled!");
    }
    else
    {
        if (thread_hard_limit <= thread_soft_limit || thread_hard_limit > OS_THREAD_SOFT_LIMIT) /// the general soft limit of OS threads is no more than 100000.
        {
            thread_hard_limit = 10000;
            thread_soft_limit = 5000;
            LOG_FMT_INFO(log, "hard limit {} should > soft limit {} and under maximum {}, so MinTSOScheduler set them as {}, {} by default, and active_set_soft_limit is {}.", hard_limit, soft_limit, OS_THREAD_SOFT_LIMIT, thread_hard_limit, thread_soft_limit, active_set_soft_limit);
        }
        else
        {
            LOG_FMT_INFO(log, "thread_hard_limit is {}, thread_soft_limit is {}, and active_set_soft_limit is {} in MinTSOScheduler.", thread_hard_limit, thread_soft_limit, active_set_soft_limit);
        }
        GET_METRIC(tiflash_task_scheduler, type_min_tso).Set(min_tso);
        GET_METRIC(tiflash_task_scheduler, type_thread_soft_limit).Set(thread_soft_limit);
        GET_METRIC(tiflash_task_scheduler, type_thread_hard_limit).Set(thread_hard_limit);
        GET_METRIC(tiflash_task_scheduler, type_estimated_thread_usage).Set(estimated_thread_usage);
        GET_METRIC(tiflash_task_scheduler, type_waiting_queries_count).Set(0);
        GET_METRIC(tiflash_task_scheduler, type_active_queries_count).Set(0);
        GET_METRIC(tiflash_task_scheduler, type_waiting_tasks_count).Set(0);
        GET_METRIC(tiflash_task_scheduler, type_active_tasks_count).Set(0);
        GET_METRIC(tiflash_task_scheduler, type_hard_limit_exceeded_count).Set(0);
    }
}

bool MinTSOScheduler::tryToSchedule(const MPPTaskPtr & task, MPPTaskManager & task_manager)
{
    /// check whether this schedule is disabled or not
    if (isDisabled())
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
    bool has_error = false;
    return scheduleImp(id.start_ts, query_task_set, task, false, has_error);
}

/// after finishing the query, there would be no threads released soon, so the updated min-tso query with waiting tasks should be scheduled.
/// the cancelled query maybe hang, so trigger scheduling as needed when deleting cancelled query.
void MinTSOScheduler::deleteQuery(const UInt64 tso, MPPTaskManager & task_manager, const bool is_cancelled)
{
    if (isDisabled())
    {
        return;
    }

    LOG_FMT_DEBUG(log, "{} query {} (is min = {}) is deleted from active set {} left {} or waiting set {} left {}.", is_cancelled ? "Cancelled" : "Finished", tso, tso == min_tso, active_set.find(tso) != active_set.end(), active_set.size(), waiting_set.find(tso) != waiting_set.end(), waiting_set.size());
    active_set.erase(tso);
    waiting_set.erase(tso);
    GET_METRIC(tiflash_task_scheduler, type_waiting_queries_count).Set(waiting_set.size());
    GET_METRIC(tiflash_task_scheduler, type_active_queries_count).Set(active_set.size());

    if (is_cancelled) /// cancelled queries may have waiting tasks, and finished queries haven't.
    {
        auto query_task_set = task_manager.getQueryTaskSetWithoutLock(tso);
        if (query_task_set) /// release all waiting tasks
        {
            while (!query_task_set->waiting_tasks.empty())
            {
                auto task_it = query_task_set->task_map.find(query_task_set->waiting_tasks.front());
                if (task_it != query_task_set->task_map.end() && task_it->second != nullptr)
                    task_it->second->scheduleThisTask(MPPTask::ScheduleState::FAILED);
                query_task_set->waiting_tasks.pop();
                GET_METRIC(tiflash_task_scheduler, type_waiting_tasks_count).Decrement();
            }
        }
    }

    /// NOTE: if updated min_tso query has waiting tasks, they should be scheduled, especially when the soft-limited threads are amost used and active tasks are in resources deadlock which cannot release threads soon.
    if (updateMinTSO(tso, true, is_cancelled ? "when cancelling it" : "as finishing it"))
    {
        scheduleWaitingQueries(task_manager);
    }
}

/// NOTE: should not throw exceptions due to being called when destruction.
void MinTSOScheduler::releaseThreadsThenSchedule(const int needed_threads, MPPTaskManager & task_manager)
{
    if (isDisabled())
    {
        return;
    }

    if (static_cast<Int64>(estimated_thread_usage) < needed_threads)
    {
        LOG_FMT_FATAL(log, "estimated_thread_usage should not be smaller than 0, actually is {}.", static_cast<Int64>(estimated_thread_usage) - needed_threads);
        std::terminate();
    }
    estimated_thread_usage -= needed_threads;
    GET_METRIC(tiflash_task_scheduler, type_estimated_thread_usage).Set(estimated_thread_usage);
    GET_METRIC(tiflash_task_scheduler, type_active_tasks_count).Decrement();
    /// as tasks release some threads, so some tasks would get scheduled.
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
            updateMinTSO(current_query_id, true, "as it is not in the task manager.");
            active_set.erase(current_query_id);
            waiting_set.erase(current_query_id);
            GET_METRIC(tiflash_task_scheduler, type_waiting_queries_count).Set(waiting_set.size());
            GET_METRIC(tiflash_task_scheduler, type_active_queries_count).Set(active_set.size());
            continue;
        }

        LOG_FMT_DEBUG(log, "query {} (is min = {}) with {} tasks is to be scheduled from waiting set (size = {}).", current_query_id, current_query_id == min_tso, query_task_set->waiting_tasks.size(), waiting_set.size());
        /// schedule tasks one by one
        while (!query_task_set->waiting_tasks.empty())
        {
            auto task_it = query_task_set->task_map.find(query_task_set->waiting_tasks.front());
            bool has_error = false;
            if (task_it != query_task_set->task_map.end() && task_it->second != nullptr && !scheduleImp(current_query_id, query_task_set, task_it->second, true, has_error))
            {
                if (has_error)
                {
                    query_task_set->waiting_tasks.pop(); /// it should be pop from the waiting queue, because the task is scheduled with errors.
                    GET_METRIC(tiflash_task_scheduler, type_waiting_tasks_count).Decrement();
                }
                return;
            }
            query_task_set->waiting_tasks.pop();
            GET_METRIC(tiflash_task_scheduler, type_waiting_tasks_count).Decrement();
        }
        LOG_FMT_DEBUG(log, "query {} (is min = {}) is scheduled from waiting set (size = {}).", current_query_id, current_query_id == min_tso, waiting_set.size());
        waiting_set.erase(current_query_id); /// all waiting tasks of this query are fully active
        GET_METRIC(tiflash_task_scheduler, type_waiting_queries_count).Set(waiting_set.size());
    }
}

/// [directly schedule, from waiting set] * [is min_tso query, not] * [can schedule, can't] totally 8 cases.
bool MinTSOScheduler::scheduleImp(const UInt64 tso, const MPPQueryTaskSetPtr & query_task_set, const MPPTaskPtr & task, const bool isWaiting, bool & has_error)
{
    auto needed_threads = task->getNeededThreads();
    auto check_for_new_min_tso = tso <= min_tso && estimated_thread_usage + needed_threads <= thread_hard_limit;
    auto check_for_not_min_tso = (active_set.size() < active_set_soft_limit || active_set.find(tso) != active_set.end()) && (estimated_thread_usage + needed_threads <= thread_soft_limit);
    if (check_for_new_min_tso || check_for_not_min_tso)
    {
        updateMinTSO(tso, false, isWaiting ? "from the waiting set" : "when directly schedule it");
        active_set.insert(tso);
        if (task->scheduleThisTask(MPPTask::ScheduleState::SCHEDULED))
        {
            estimated_thread_usage += needed_threads;
            GET_METRIC(tiflash_task_scheduler, type_active_tasks_count).Increment();
        }
        GET_METRIC(tiflash_task_scheduler, type_active_queries_count).Set(active_set.size());
        GET_METRIC(tiflash_task_scheduler, type_estimated_thread_usage).Set(estimated_thread_usage);
        LOG_FMT_INFO(log, "{} is scheduled (active set size = {}) due to available threads {}, after applied for {} threads, used {} of the thread {} limit {}.", task->getId().toString(), active_set.size(), isWaiting ? " from the waiting set" : " directly", needed_threads, estimated_thread_usage, min_tso == tso ? "hard" : "soft", min_tso == tso ? thread_hard_limit : thread_soft_limit);
        return true;
    }
    else
    {
        if (tso <= min_tso) /// the min_tso query should fully run, otherwise throw errors here.
        {
            has_error = true;
            auto msg = fmt::format("threads are unavailable for the query {} ({} min_tso {}) {}, need {}, but used {} of the thread hard limit {}, {} active and {} waiting queries.", tso, tso == min_tso ? "is" : "is newer than", min_tso, isWaiting ? "from the waiting set" : "when directly schedule it", needed_threads, estimated_thread_usage, thread_hard_limit, active_set.size(), waiting_set.size());
            LOG_FMT_ERROR(log, "{}", msg);
            GET_METRIC(tiflash_task_scheduler, type_hard_limit_exceeded_count).Increment();
            if (isWaiting)
            {
                /// set this task be failed to schedule, and the task will throw exception, then TiDB will finally notify this tiflash node canceling all tasks of this tso and update metrics.
                task->scheduleThisTask(MPPTask::ScheduleState::EXCEEDED);
                waiting_set.erase(tso); /// avoid the left waiting tasks of this query reaching here many times.
            }
            else
            {
                throw Exception(msg);
            }
            return false;
        }
        if (!isWaiting)
        {
            waiting_set.insert(tso);
            query_task_set->waiting_tasks.push(task->getId());
            GET_METRIC(tiflash_task_scheduler, type_waiting_queries_count).Set(waiting_set.size());
            GET_METRIC(tiflash_task_scheduler, type_waiting_tasks_count).Increment();
        }
        LOG_FMT_INFO(log, "threads are unavailable for the query {} or active set is full (size =  {}), need {}, but used {} of the thread soft limit {},{} waiting set size = {}", tso, active_set.size(), needed_threads, estimated_thread_usage, thread_soft_limit, isWaiting ? "" : " put into", waiting_set.size());
        return false;
    }
}

/// if return true, then need to schedule the waiting tasks of the min_tso.
bool MinTSOScheduler::updateMinTSO(const UInt64 tso, const bool retired, const String msg)
{
    auto old_min_tso = min_tso;
    bool force_scheduling = false;
    if (retired)
    {
        if (tso == min_tso) /// elect a new min_tso from all queries.
        {
            min_tso = active_set.empty() ? MAX_UINT64 : *active_set.begin();
            min_tso = waiting_set.empty() ? min_tso : std::min(*waiting_set.begin(), min_tso);
            force_scheduling = waiting_set.find(min_tso) != waiting_set.end(); /// if this min_tso has waiting tasks, these tasks should force being scheduled.
        }
    }
    else
    {
        min_tso = std::min(tso, min_tso);
    }
    if (min_tso != old_min_tso) /// if min_tso == MAX_UINT64 and the query tso is not to be cancelled, the used_threads, active_set.size() and waiting_set.size() must be 0.
    {
        GET_METRIC(tiflash_task_scheduler, type_min_tso).Set(min_tso);
        LOG_FMT_INFO(log, "min_tso query is updated from {} to {} {}, used threads = {}, {} active and {} waiting queries.", old_min_tso, min_tso, msg, estimated_thread_usage, active_set.size(), waiting_set.size());
    }
    return force_scheduling;
}

} // namespace DB
