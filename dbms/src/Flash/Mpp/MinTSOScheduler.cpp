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
    : thread_soft_limit(soft_limit)
    , thread_hard_limit(hard_limit)
    , global_estimated_thread_usage(0)
    , active_set_soft_limit(active_set_soft_limit_)
    , log(Logger::get())
{
    auto cores = static_cast<size_t>(getNumberOfLogicalCPUCores());
    if (active_set_soft_limit == 0)
    {
        /// set active_set_soft_limit to a reasonable value
        active_set_soft_limit = std::max(2 * cores, 1); /// at least 1
    }
    if (isDisabled())
    {
        LOG_WARNING(log, "MinTSOScheduler is disabled!");
    }
    else
    {
        if (thread_hard_limit <= thread_soft_limit
            || thread_hard_limit
                > OS_THREAD_SOFT_LIMIT) /// the general soft limit of OS threads is no more than 100000.
        {
            thread_hard_limit = 10000;
            thread_soft_limit = 5000;
            LOG_WARNING(
                log,
                "hard limit {} should > soft limit {} and under maximum {}, so MinTSOScheduler set them as {}, {} by "
                "default, and active_set_soft_limit is {}.",
                hard_limit,
                soft_limit,
                OS_THREAD_SOFT_LIMIT,
                thread_hard_limit,
                thread_soft_limit,
                active_set_soft_limit);
        }
        else
        {
            LOG_INFO(
                log,
                "thread_hard_limit is {}, thread_soft_limit is {}, and active_set_soft_limit is {} in MinTSOScheduler.",
                thread_hard_limit,
                thread_soft_limit,
                active_set_soft_limit);
        }
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
    auto [query_task_set, aborted_reason] = task_manager.getGatherTaskSetWithoutLock(id.gather_id);
    if (nullptr == query_task_set || !aborted_reason.empty())
    {
        LOG_WARNING(log, "{} is scheduled with miss or abort.", id.toString());
        return true;
    }
    bool has_error = false;
    return scheduleImp(id.gather_id.query_id, query_task_set, schedule_entry, false, has_error);
}

/// after finishing the query, there would be no threads released soon, so the updated min-query-id query with waiting tasks should be scheduled.
/// the cancelled query maybe hang, so trigger scheduling as needed when deleting cancelled query.
void MinTSOScheduler::deleteQuery(
    const MPPQueryId & query_id,
    MPPTaskManager & task_manager,
    bool is_cancelled,
    Int64 gather_id)
{
    if (isDisabled())
    {
        return;
    }

    auto & group_entry = getOrCreateGroupEntry(query_id.resource_group_name);
    bool all_gathers_deleted = true;
    auto query = task_manager.getMPPQueryWithoutLock(query_id);

    if (query != nullptr) /// release all waiting tasks
    {
        for (const auto & gather_it : query->mpp_gathers)
        {
            if (gather_id == -1 || gather_it.first.gather_id == gather_id)
            {
                if (is_cancelled) /// cancelled queries may have waiting tasks, and finished queries haven't.
                {
                    while (!gather_it.second->waiting_tasks.empty())
                    {
                        auto * task = gather_it.second->findMPPTask(gather_it.second->waiting_tasks.front());
                        if (task != nullptr)
                            task->scheduleThisTask(ScheduleState::FAILED);
                        gather_it.second->waiting_tasks.pop();
                        GET_RESOURCE_GROUP_METRIC(
                            tiflash_task_scheduler,
                            type_waiting_tasks_count,
                            query_id.resource_group_name)
                            .Decrement();
                    }
                }
            }
            else
            {
                all_gathers_deleted = false;
            }
        }
    }

    if (all_gathers_deleted)
    {
        LOG_DEBUG(
            log,
            "{} query {} (is min = {}) is deleted from active set {} left {} or waiting set {} left {}.",
            is_cancelled ? "Cancelled" : "Finished",
            query_id.toString(),
            query_id == group_entry.min_query_id,
            group_entry.active_set.find(query_id) != group_entry.active_set.end(),
            group_entry.active_set.size(),
            group_entry.waiting_set.find(query_id) != group_entry.waiting_set.end(),
            group_entry.waiting_set.size());
        group_entry.active_set.erase(query_id);
        group_entry.waiting_set.erase(query_id);
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_waiting_queries_count, group_entry.resource_group_name)
            .Set(group_entry.waiting_set.size());
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_active_queries_count, group_entry.resource_group_name)
            .Set(group_entry.active_set.size());

        /// NOTE: if updated min_query_id query has waiting tasks, they should be scheduled, especially when the soft-limited threads are amost used and active tasks are in resources deadlock which cannot release threads soon.
        if (group_entry.updateMinQueryId(query_id, true, is_cancelled ? "when cancelling it" : "as finishing it", log))
        {
            scheduleWaitingQueries(group_entry, task_manager, log);
        }
    }
    else
    {
        LOG_DEBUG(
            log,
            "{} gather {} of query {}, and there are still some other gathers remain",
            is_cancelled ? "Cancelled" : "Finished",
            gather_id,
            query_id.toString());
    }
}

MPPQueryId MinTSOScheduler::getCurrentMinTSOQueryId(const String & resource_group_name)
{
    auto & group_entry = getOrCreateGroupEntry(resource_group_name);
    return group_entry.min_query_id;
}

/// NOTE: should not throw exceptions due to being called when destruction.
void MinTSOScheduler::releaseThreadsThenSchedule(
    const String & resource_group_name,
    int needed_threads,
    MPPTaskManager & task_manager)
{
    if (isDisabled())
    {
        return;
    }

    global_estimated_thread_usage -= needed_threads;
    auto & group_entry = mustGetGroupEntry(resource_group_name);
    auto updated_estimated_threads = static_cast<Int64>(group_entry.estimated_thread_usage) - needed_threads;
    RUNTIME_ASSERT(
        updated_estimated_threads >= 0,
        log,
        "estimated_thread_usage should not be smaller than 0, actually is {}.",
        updated_estimated_threads);

    group_entry.estimated_thread_usage = updated_estimated_threads;
    GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_estimated_thread_usage, resource_group_name)
        .Set(group_entry.estimated_thread_usage);
    GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_active_tasks_count, resource_group_name).Decrement();
    /// as tasks release some threads, so some tasks would get scheduled.
    scheduleWaitingQueries(group_entry, task_manager, log);
    if ((group_entry.active_set.size() + group_entry.waiting_set.size()) == 0
        && group_entry.estimated_thread_usage == 0)
    {
        LOG_DEBUG(log, "min tso scheduler_entry of resouce group {} deleted", resource_group_name);
        group_entries.erase(resource_group_name);
        GET_METRIC(tiflash_task_scheduler, type_group_entry_count).Decrement();
    }
}

void MinTSOScheduler::scheduleWaitingQueries(GroupEntry & group_entry, MPPTaskManager & task_manager, LoggerPtr log)
{
    /// schedule new tasks
    while (!group_entry.waiting_set.empty())
    {
        auto current_query_id = *group_entry.waiting_set.begin();
        auto query = task_manager.getMPPQueryWithoutLock(current_query_id);
        if (nullptr == query) /// silently solve this rare case
        {
            LOG_ERROR(log, "the waiting query {} is not in the task manager.", current_query_id.toString());
            group_entry.updateMinQueryId(current_query_id, true, "as it is not in the task manager.", log);
            group_entry.active_set.erase(current_query_id);
            group_entry.waiting_set.erase(current_query_id);
            GET_RESOURCE_GROUP_METRIC(
                tiflash_task_scheduler,
                type_waiting_queries_count,
                group_entry.resource_group_name)
                .Set(group_entry.waiting_set.size());
            GET_RESOURCE_GROUP_METRIC(
                tiflash_task_scheduler,
                type_active_queries_count,
                group_entry.resource_group_name)
                .Set(group_entry.active_set.size());
            continue;
        }

        LOG_DEBUG(
            log,
            "query {} (is min = {}) is to be scheduled from waiting set (size = {}).",
            current_query_id.toString(),
            current_query_id == group_entry.min_query_id,
            group_entry.waiting_set.size());
        /// schedule tasks one by one
        for (auto & gather_set : query->mpp_gathers)
        {
            bool has_error = false;
            while (!gather_set.second->waiting_tasks.empty())
            {
                auto * task = gather_set.second->findMPPTask(gather_set.second->waiting_tasks.front());
                /// Only when MinTSO task reach hard limit, has_error is set true. Schedule all waiting tasks with
                /// same query id and gather id to be exceeded state.
                if (has_error)
                {
                    if (task != nullptr)
                        task->getScheduleEntry().schedule(ScheduleState::EXCEEDED);
                    gather_set.second->waiting_tasks.pop();
                    GET_RESOURCE_GROUP_METRIC(
                        tiflash_task_scheduler,
                        type_waiting_tasks_count,
                        group_entry.resource_group_name)
                        .Decrement();
                    continue;
                }

                if (task != nullptr)
                {
                    bool scheduled
                        = scheduleImp(current_query_id, gather_set.second, task->getScheduleEntry(), true, has_error);
                    if (!scheduled && !has_error)
                        return;
                }
                gather_set.second->waiting_tasks.pop();
                GET_RESOURCE_GROUP_METRIC(
                    tiflash_task_scheduler,
                    type_waiting_tasks_count,
                    group_entry.resource_group_name)
                    .Decrement();
            }
        }
        LOG_DEBUG(
            log,
            "query {} (is min = {}) is scheduled from waiting set (size = {}).",
            current_query_id.toString(),
            current_query_id == group_entry.min_query_id,
            group_entry.waiting_set.size());
        group_entry.waiting_set.erase(current_query_id); /// all waiting tasks of this query are fully active
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_waiting_queries_count, group_entry.resource_group_name)
            .Set(group_entry.waiting_set.size());
    }
}

/// [directly schedule, from waiting set] * [is min_query_id query, not] * [can schedule, can't] totally 8 cases.
bool MinTSOScheduler::scheduleImp(
    const MPPQueryId & query_id,
    const MPPGatherTaskSetPtr & query_task_set,
    MPPTaskScheduleEntry & schedule_entry,
    bool isWaiting,
    bool & has_error)
{
    auto needed_threads = schedule_entry.getNeededThreads();
    auto & group_entry = getOrCreateGroupEntry(query_id.resource_group_name);

    auto check_for_new_min_tso
        = query_id <= group_entry.min_query_id && global_estimated_thread_usage + needed_threads <= thread_hard_limit;
    auto check_for_not_min_tso = (group_entry.active_set.size() < active_set_soft_limit
                                  || group_entry.active_set.find(query_id) != group_entry.active_set.end())
        && (group_entry.estimated_thread_usage + needed_threads <= thread_soft_limit);
    if (check_for_new_min_tso || check_for_not_min_tso)
    {
        group_entry
            .updateMinQueryId(query_id, false, isWaiting ? "from the waiting set" : "when directly schedule it", log);
        group_entry.active_set.insert(query_id);
        if (schedule_entry.schedule(ScheduleState::SCHEDULED))
        {
            group_entry.estimated_thread_usage += needed_threads;
            global_estimated_thread_usage += needed_threads;
            GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_active_tasks_count, group_entry.resource_group_name)
                .Increment();
        }
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_active_queries_count, group_entry.resource_group_name)
            .Set(group_entry.active_set.size());
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_estimated_thread_usage, group_entry.resource_group_name)
            .Set(group_entry.estimated_thread_usage);
        GET_METRIC(tiflash_task_scheduler, type_global_estimated_thread_usage).Set(global_estimated_thread_usage);
        LOG_DEBUG(
            log,
            "{} is scheduled (active set size = {}) due to available threads {}, after applied for {} threads, used {} "
            "of the thread {} limit {}.",
            schedule_entry.getMPPTaskId().toString(),
            group_entry.active_set.size(),
            isWaiting ? "from the waiting set" : "directly",
            needed_threads,
            group_entry.estimated_thread_usage,
            group_entry.min_query_id == query_id ? "hard" : "soft",
            group_entry.min_query_id == query_id ? thread_hard_limit : thread_soft_limit);
        return true;
    }
    else
    {
        bool is_query_id_min = query_id <= group_entry.min_query_id;
        fiu_do_on(FailPoints::random_min_tso_scheduler_failpoint, is_query_id_min = true;);
        if (is_query_id_min) /// the min_query_id query should fully run, otherwise throw errors here.
        {
            has_error = true;
            auto msg = fmt::format(
                "threads are unavailable for the query {} ({} min_query_id {}) {}, need {}, but used {} of the thread "
                "hard limit {}, {} active and {} waiting queries.",
                query_id.toString(),
                query_id == group_entry.min_query_id ? "is" : "is newer than",
                group_entry.min_query_id.toString(),
                isWaiting ? "from the waiting set" : "when directly schedule it",
                needed_threads,
                group_entry.estimated_thread_usage,
                thread_hard_limit,
                group_entry.active_set.size(),
                group_entry.waiting_set.size());
            LOG_ERROR(log, "{}", msg);
            GET_RESOURCE_GROUP_METRIC(
                tiflash_task_scheduler,
                type_hard_limit_exceeded_count,
                group_entry.resource_group_name)
                .Increment();
            if (isWaiting)
            {
                /// set this task be failed to schedule, and the task will throw exception.
                schedule_entry.schedule(ScheduleState::EXCEEDED);
            }
            else
            {
                throw Exception(msg);
            }
            return false;
        }
        if (!isWaiting)
        {
            group_entry.waiting_set.insert(query_id);
            query_task_set->waiting_tasks.push(schedule_entry.getMPPTaskId());
            GET_RESOURCE_GROUP_METRIC(
                tiflash_task_scheduler,
                type_waiting_queries_count,
                group_entry.resource_group_name)
                .Set(group_entry.waiting_set.size());
            GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_waiting_tasks_count, group_entry.resource_group_name)
                .Increment();
        }
        LOG_INFO(
            log,
            "Resource temporary not available for query {}(is first schedule: {}), available threads count are {}, "
            "available active set size = {}, "
            "required threads count are {}, waiting set size = {}",
            query_id.toString(),
            !isWaiting,
            thread_soft_limit - group_entry.estimated_thread_usage,
            active_set_soft_limit - group_entry.active_set.size(),
            needed_threads,
            group_entry.waiting_set.size());
        return false;
    }
}

/// if return true, then need to schedule the waiting tasks of the min_query_id.
bool MinTSOScheduler::GroupEntry::updateMinQueryId(
    const MPPQueryId & query_id,
    bool retired,
    const String & msg,
    LoggerPtr log)
{
    auto old_min_query_id = min_query_id;
    bool force_scheduling = false;
    if (retired)
    {
        if (query_id == min_query_id) /// elect a new min_query_id from all queries.
        {
            min_query_id = active_set.empty() ? MPPTaskId::Max_Query_Id : *active_set.begin();
            min_query_id = waiting_set.empty() ? min_query_id : std::min(*waiting_set.begin(), min_query_id);
            force_scheduling = waiting_set.find(min_query_id)
                != waiting_set
                       .end(); /// if this min_query_id has waiting tasks, these tasks should force being scheduled.
        }
    }
    else
    {
        min_query_id = std::min(query_id, min_query_id);
    }
    if (min_query_id
        != old_min_query_id) /// if min_query_id == MPPTaskId::Max_Query_Id and the query_id is not to be cancelled, the used_threads, active_set.size() and waiting_set.size() must be 0.
    {
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_min_tso, resource_group_name)
            .Set(min_query_id.query_ts == 0 ? min_query_id.start_ts : min_query_id.query_ts);
        LOG_DEBUG(
            log,
            "min_query_id query is updated from {} to {} {}, used threads = {}, {} active and {} waiting queries.",
            old_min_query_id.toString(),
            min_query_id.toString(),
            msg,
            estimated_thread_usage,
            active_set.size(),
            waiting_set.size());
    }
    return force_scheduling;
}

MinTSOScheduler::GroupEntry & MinTSOScheduler::getOrCreateGroupEntry(const String & resource_group_name)
{
    auto iter = group_entries.find(resource_group_name);
    if (iter == group_entries.end())
    {
        iter = group_entries.insert({resource_group_name, GroupEntry(resource_group_name)}).first;
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_min_tso, resource_group_name)
            .Set(iter->second.min_query_id.query_ts);
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_thread_hard_limit, resource_group_name)
            .Set(thread_hard_limit);
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_thread_soft_limit, resource_group_name)
            .Set(thread_soft_limit);
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_estimated_thread_usage, resource_group_name).Set(0);
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_waiting_queries_count, resource_group_name).Set(0);
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_active_queries_count, resource_group_name).Set(0);
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_waiting_tasks_count, resource_group_name).Set(0);
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_active_tasks_count, resource_group_name).Set(0);
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler, type_hard_limit_exceeded_count, resource_group_name).Set(0);
        GET_METRIC(tiflash_task_scheduler, type_group_entry_count).Increment();
    }
    return iter->second;
}

MinTSOScheduler::GroupEntry & MinTSOScheduler::mustGetGroupEntry(const String & resource_group_name)
{
    auto iter = group_entries.find(resource_group_name);
    RUNTIME_CHECK_MSG(
        iter != group_entries.end(),
        "cannot find min tso scheduler for resource group {}",
        resource_group_name);
    return iter->second;
}

} // namespace DB
