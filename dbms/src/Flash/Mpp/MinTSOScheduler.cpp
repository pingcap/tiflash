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

#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/getNumberOfCPUCores.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MinTSOScheduler.h>

namespace DB
{
namespace FailPoints
{
extern const char random_min_tso_scheduler_failpoint[];
} // namespace FailPoints

constexpr UInt64 OS_THREAD_SOFT_LIMIT = 100000;

MinTSOScheduler::MinTSOScheduler(UInt64 soft_limit, UInt64 hard_limit, UInt64 active_set_soft_limit_)
    : min_query_id(MPPTaskId::Max_Query_Id)
    , thread_soft_limit(soft_limit)
    , thread_hard_limit(hard_limit)
    , estimated_thread_usage(0)
    , active_set_soft_limit(active_set_soft_limit_)
    , log(Logger::get())
{
    auto cores = static_cast<size_t>(getNumberOfLogicalCPUCores() / 2);
    if (active_set_soft_limit == 0)
    {
        /// set active_set_soft_limit to a reasonable value
        active_set_soft_limit = (cores + 2) / 2; /// at least 1
    }
    if (isDisabled())
    {
        LOG_WARNING(log, "MinTSOScheduler is disabled!");
    }
    else
    {
        if (thread_hard_limit <= thread_soft_limit || thread_hard_limit > OS_THREAD_SOFT_LIMIT) /// the general soft limit of OS threads is no more than 100000.
        {
            thread_hard_limit = 10000;
            thread_soft_limit = 5000;
            LOG_WARNING(log, "hard limit {} should > soft limit {} and under maximum {}, so MinTSOScheduler set them as {}, {} by default, and active_set_soft_limit is {}.", hard_limit, soft_limit, OS_THREAD_SOFT_LIMIT, thread_hard_limit, thread_soft_limit, active_set_soft_limit);
        }
        else
        {
            LOG_INFO(log, "thread_hard_limit is {}, thread_soft_limit is {}, and active_set_soft_limit is {} in MinTSOScheduler.", thread_hard_limit, thread_soft_limit, active_set_soft_limit);
        }
        GET_METRIC(tiflash_task_scheduler, type_min_tso).Set(min_query_id.query_ts);
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

bool MinTSOScheduler::tryToSchedule(MPPTaskScheduleEntry & schedule_entry, MPPTaskManager & task_manager)
{
    /// check whether this schedule is disabled or not
    if (isDisabled())
    {
        return true;
    }
    const auto & id = schedule_entry.getMPPTaskId();
    auto query_task_set = task_manager.getQueryTaskSetWithoutLock(id.query_id);
    if (nullptr == query_task_set || !query_task_set->isInNormalState())
    {
        LOG_WARNING(log, "{} is scheduled with miss or abort.", id.toString());
        return true;
    }
    bool has_error = false;
    return scheduleImp(id.query_id, query_task_set, schedule_entry, false, has_error);
}

/// after finishing the query, there would be no threads released soon, so the updated min-query-id query with waiting tasks should be scheduled.
/// the cancelled query maybe hang, so trigger scheduling as needed when deleting cancelled query.
void MinTSOScheduler::deleteQuery(const MPPQueryId & query_id, MPPTaskManager & task_manager, const bool is_cancelled)
{
    if (isDisabled())
    {
        return;
    }

    LOG_DEBUG(log, "{} query {} (is min = {}) is deleted from active set {} left {} or waiting set {} left {}.", is_cancelled ? "Cancelled" : "Finished", query_id.toString(), query_id == min_query_id, active_set.find(query_id) != active_set.end(), active_set.size(), waiting_set.find(query_id) != waiting_set.end(), waiting_set.size());
    active_set.erase(query_id);
    waiting_set.erase(query_id);
    GET_METRIC(tiflash_task_scheduler, type_waiting_queries_count).Set(waiting_set.size());
    GET_METRIC(tiflash_task_scheduler, type_active_queries_count).Set(active_set.size());

    if (is_cancelled) /// cancelled queries may have waiting tasks, and finished queries haven't.
    {
        auto query_task_set = task_manager.getQueryTaskSetWithoutLock(query_id);
        if (query_task_set) /// release all waiting tasks
        {
            while (!query_task_set->waiting_tasks.empty())
            {
                auto task_it = query_task_set->task_map.find(query_task_set->waiting_tasks.front());
                if (task_it != query_task_set->task_map.end() && task_it->second != nullptr)
                    task_it->second->scheduleThisTask(ScheduleState::FAILED);
                query_task_set->waiting_tasks.pop();
                GET_METRIC(tiflash_task_scheduler, type_waiting_tasks_count).Decrement();
            }
        }
    }

    /// NOTE: if updated min_query_id query has waiting tasks, they should be scheduled, especially when the soft-limited threads are amost used and active tasks are in resources deadlock which cannot release threads soon.
    if (updateMinQueryId(query_id, true, is_cancelled ? "when cancelling it" : "as finishing it"))
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

    auto updated_estimated_threads = static_cast<Int64>(estimated_thread_usage) - needed_threads;
    RUNTIME_ASSERT(updated_estimated_threads >= 0, log, "estimated_thread_usage should not be smaller than 0, actually is {}.", updated_estimated_threads);

    estimated_thread_usage = updated_estimated_threads;
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
            LOG_ERROR(log, "the waiting query {} is not in the task manager.", current_query_id.toString());
            updateMinQueryId(current_query_id, true, "as it is not in the task manager.");
            active_set.erase(current_query_id);
            waiting_set.erase(current_query_id);
            GET_METRIC(tiflash_task_scheduler, type_waiting_queries_count).Set(waiting_set.size());
            GET_METRIC(tiflash_task_scheduler, type_active_queries_count).Set(active_set.size());
            continue;
        }

        LOG_DEBUG(log, "query {} (is min = {}) with {} tasks is to be scheduled from waiting set (size = {}).", current_query_id.toString(), current_query_id == min_query_id, query_task_set->waiting_tasks.size(), waiting_set.size());
        /// schedule tasks one by one
        while (!query_task_set->waiting_tasks.empty())
        {
            auto task_it = query_task_set->task_map.find(query_task_set->waiting_tasks.front());
            bool has_error = false;
            if (task_it != query_task_set->task_map.end() && task_it->second != nullptr && !scheduleImp(current_query_id, query_task_set, task_it->second->getScheduleEntry(), true, has_error))
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
        LOG_DEBUG(log, "query {} (is min = {}) is scheduled from waiting set (size = {}).", current_query_id.toString(), current_query_id == min_query_id, waiting_set.size());
        waiting_set.erase(current_query_id); /// all waiting tasks of this query are fully active
        GET_METRIC(tiflash_task_scheduler, type_waiting_queries_count).Set(waiting_set.size());
    }
}

/// [directly schedule, from waiting set] * [is min_query_id query, not] * [can schedule, can't] totally 8 cases.
bool MinTSOScheduler::scheduleImp(const MPPQueryId & query_id, const MPPQueryTaskSetPtr & query_task_set, MPPTaskScheduleEntry & schedule_entry, const bool isWaiting, bool & has_error)
{
    auto needed_threads = schedule_entry.getNeededThreads();
    auto check_for_new_min_tso = query_id <= min_query_id && estimated_thread_usage + needed_threads <= thread_hard_limit;
    auto check_for_not_min_tso = (active_set.size() < active_set_soft_limit || active_set.find(query_id) != active_set.end()) && (estimated_thread_usage + needed_threads <= thread_soft_limit);
    if (check_for_new_min_tso || check_for_not_min_tso)
    {
        updateMinQueryId(query_id, false, isWaiting ? "from the waiting set" : "when directly schedule it");
        active_set.insert(query_id);
        if (schedule_entry.schedule(ScheduleState::SCHEDULED))
        {
            estimated_thread_usage += needed_threads;
            GET_METRIC(tiflash_task_scheduler, type_active_tasks_count).Increment();
        }
        GET_METRIC(tiflash_task_scheduler, type_active_queries_count).Set(active_set.size());
        GET_METRIC(tiflash_task_scheduler, type_estimated_thread_usage).Set(estimated_thread_usage);
        LOG_DEBUG(log, "{} is scheduled (active set size = {}) due to available threads {}, after applied for {} threads, used {} of the thread {} limit {}.", schedule_entry.getMPPTaskId().toString(), active_set.size(), isWaiting ? "from the waiting set" : "directly", needed_threads, estimated_thread_usage, min_query_id == query_id ? "hard" : "soft", min_query_id == query_id ? thread_hard_limit : thread_soft_limit);
        return true;
    }
    else
    {
        bool is_query_id_min = query_id <= min_query_id;
        fiu_do_on(FailPoints::random_min_tso_scheduler_failpoint, is_query_id_min = true;);
        if (is_query_id_min) /// the min_query_id query should fully run, otherwise throw errors here.
        {
            has_error = true;
            auto msg = fmt::format("threads are unavailable for the query {} ({} min_query_id {}) {}, need {}, but used {} of the thread hard limit {}, {} active and {} waiting queries.", query_id.toString(), query_id == min_query_id ? "is" : "is newer than", min_query_id.toString(), isWaiting ? "from the waiting set" : "when directly schedule it", needed_threads, estimated_thread_usage, thread_hard_limit, active_set.size(), waiting_set.size());
            LOG_ERROR(log, "{}", msg);
            GET_METRIC(tiflash_task_scheduler, type_hard_limit_exceeded_count).Increment();
            if (isWaiting)
            {
                /// set this task be failed to schedule, and the task will throw exception, then TiDB will finally notify this tiflash node canceling all tasks of this query_id and update metrics.
                schedule_entry.schedule(ScheduleState::EXCEEDED);
                waiting_set.erase(query_id); /// avoid the left waiting tasks of this query reaching here many times.
            }
            else
            {
                throw Exception(msg);
            }
            return false;
        }
        if (!isWaiting)
        {
            waiting_set.insert(query_id);
            query_task_set->waiting_tasks.push(schedule_entry.getMPPTaskId());
            GET_METRIC(tiflash_task_scheduler, type_waiting_queries_count).Set(waiting_set.size());
            GET_METRIC(tiflash_task_scheduler, type_waiting_tasks_count).Increment();
        }
        LOG_INFO(log, "Resource temporary not available for query {}(is first schedule: {}), available threads count are {}, available active set size = {}, "
                      "required threads count are {}, waiting set size = {}",
                 query_id.toString(),
                 !isWaiting,
                 thread_soft_limit - estimated_thread_usage,
                 active_set_soft_limit - active_set.size(),
                 needed_threads,
                 waiting_set.size());
        return false;
    }
}

/// if return true, then need to schedule the waiting tasks of the min_query_id.
bool MinTSOScheduler::updateMinQueryId(const MPPQueryId & query_id, const bool retired, const String & msg)
{
    auto old_min_query_id = min_query_id;
    bool force_scheduling = false;
    if (retired)
    {
        if (query_id == min_query_id) /// elect a new min_query_id from all queries.
        {
            min_query_id = active_set.empty() ? MPPTaskId::Max_Query_Id : *active_set.begin();
            min_query_id = waiting_set.empty() ? min_query_id : std::min(*waiting_set.begin(), min_query_id);
            force_scheduling = waiting_set.find(min_query_id) != waiting_set.end(); /// if this min_query_id has waiting tasks, these tasks should force being scheduled.
        }
    }
    else
    {
        min_query_id = std::min(query_id, min_query_id);
    }
    if (min_query_id != old_min_query_id) /// if min_query_id == MPPTaskId::Max_Query_Id and the query_id is not to be cancelled, the used_threads, active_set.size() and waiting_set.size() must be 0.
    {
        GET_METRIC(tiflash_task_scheduler, type_min_tso).Set(min_query_id.query_ts == 0 ? min_query_id.start_ts : min_query_id.query_ts);
        LOG_DEBUG(log, "min_query_id query is updated from {} to {} {}, used threads = {}, {} active and {} waiting queries.", old_min_query_id.toString(), min_query_id.toString(), msg, estimated_thread_usage, active_set.size(), waiting_set.size());
    }
    return force_scheduling;
}

} // namespace DB
