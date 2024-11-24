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
#include <Common/FmtUtils.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <fmt/core.h>

#include <magic_enum.hpp>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

namespace DB
{
namespace FailPoints
{
extern const char random_task_manager_find_task_failure_failpoint[];
extern const char pause_before_make_non_root_mpp_task_active[];
extern const char pause_before_register_non_root_mpp_task[];
} // namespace FailPoints

MPPTask * MPPGatherTaskSet::findMPPTask(const MPPTaskId & task_id) const
{
    const auto & it = task_map.find(task_id);
    if (it == task_map.end())
        return nullptr;
    return it->second.get();
}

void MPPGatherTaskSet::cancelAlarmsBySenderTaskId(const MPPTaskId & task_id)
{
    /// cancel all the alarm waiting on this task
    auto alarm_it = alarms.find(task_id.task_id);
    if (alarm_it != alarms.end())
    {
        for (auto & alarm : alarm_it->second)
            alarm.second.Cancel();
        alarms.erase(alarm_it);
    }
}

void MPPGatherTaskSet::markTaskAsFinishedOrFailed(const MPPTaskId & task_id, const String & error_message)
{
    task_map.erase(task_id);
    finished_or_failed_tasks[task_id] = error_message;
    /// cancel all the alarms on this task
    cancelAlarmsBySenderTaskId(task_id);
}

MPPQuery::~MPPQuery()
{
    if likely (process_list_entry != nullptr)
    {
        auto peak_memory = process_list_entry->get().getMemoryTrackerPtr()->getPeak();
        GET_METRIC(tiflash_coprocessor_request_memory_usage, type_run_mpp_query).Observe(peak_memory);
    }
}

MPPGatherTaskSetPtr MPPQuery::addMPPGatherTaskSet(const MPPGatherId & gather_id)
{
    auto ptr = std::make_shared<MPPGatherTaskSet>();
    mpp_gathers.insert({gather_id, ptr});
    return ptr;
}

MPPTaskManager::MPPTaskManager(MPPTaskSchedulerPtr scheduler_)
    : scheduler(std::move(scheduler_))
    , aborted_query_gather_cache(ABORTED_MPPGATHER_CACHE_SIZE)
    , log(Logger::get())
    , monitor(std::make_shared<MPPTaskMonitor>(log))
{}

MPPTaskManager::~MPPTaskManager()
{
    shutdown();
}

void MPPTaskManager::shutdown()
{
    if (monitor)
    {
        std::lock_guard lock(monitor->mu);
        monitor->is_shutdown = true;
        monitor->cv.notify_all();
    }
}

MPPQueryPtr MPPTaskManager::addMPPQuery(
    const MPPQueryId & query_id,
    bool has_meaningful_gather_id,
    UInt64 auto_spill_check_min_interval_ms)
{
    auto ptr = std::make_shared<MPPQuery>(query_id, has_meaningful_gather_id, auto_spill_check_min_interval_ms);
    mpp_query_map.insert({query_id, ptr});
    GET_METRIC(tiflash_mpp_task_manager, type_mpp_query_count).Set(mpp_query_map.size());
    return ptr;
}

MPPQueryPtr MPPTaskManager::getMPPQueryWithoutLock(const DB::MPPQueryId & query_id)
{
    const auto & query_it = mpp_query_map.find(query_id);
    return query_it == mpp_query_map.end() ? nullptr : query_it->second;
}

MPPQueryPtr MPPTaskManager::getMPPQuery(const MPPQueryId & query_id)
{
    std::unique_lock lock(mu);
    return getMPPQueryWithoutLock(query_id);
}

void MPPTaskManager::removeMPPGatherTaskSet(MPPQueryPtr & query, const MPPGatherId & gather_id, bool on_abort)
{
    query->mpp_gathers.erase(gather_id);
    if (query->mpp_gathers.empty())
    {
        scheduler->deleteQuery(gather_id.query_id, *this, on_abort, -1);
        mpp_query_map.erase(gather_id.query_id);
        GET_METRIC(tiflash_mpp_task_manager, type_mpp_query_count).Set(mpp_query_map.size());
    }
}

std::pair<MPPTunnelPtr, String> MPPTaskManager::findAsyncTunnel(
    const ::mpp::EstablishMPPConnectionRequest * request,
    EstablishCallData * call_data,
    grpc::CompletionQueue * cq,
    const Context & context)
{
    const auto & meta = request->sender_meta();
    MPPTaskId id{meta};
    Int64 sender_task_id = meta.task_id();
    Int64 receiver_task_id = request->receiver_meta().task_id();
    String req_info = fmt::format("tunnel{}+{}", request->sender_meta().task_id(), request->receiver_meta().task_id());

    std::unique_lock lock(mu);
    auto [query, gather_task_set, error_msg] = getMPPQueryAndGatherTaskSet(id.gather_id);
    if (!error_msg.empty())
    {
        /// if the gather is aborted, return the error message
        LOG_WARNING(log, "{}: Gather <{}> is aborted, all its tasks are invalid.", req_info, id.gather_id.toString());
        /// meet error
        return {nullptr, error_msg};
    }

    auto * task = gather_task_set == nullptr ? nullptr : gather_task_set->findMPPTask(id);
    if (task == nullptr)
    {
        /// task not found or not visible yet
        if (!call_data->isWaitingTunnelState())
        {
            if (gather_task_set != nullptr)
            {
                auto task_result_info = gather_task_set->isTaskAlreadyFinishedOrFailed(id);
                if (task_result_info.first)
                    return {
                        nullptr,
                        task_result_info.second.empty()
                            ? fmt::format("Task {} is already finished", id.task_id)
                            : fmt::format("Task {} is failed: {}", id.task_id, task_result_info.second)};
            }
            /// if call_data is in new_request state, put it to waiting tunnel state
            if (query == nullptr)
                query = addMPPQuery(
                    id.gather_id.query_id,
                    id.gather_id.hasMeaningfulGatherId(),
                    context.getSettingsRef().auto_spill_check_min_interval_ms.get());
            if (gather_task_set == nullptr)
                gather_task_set = query->addMPPGatherTaskSet(id.gather_id);
            auto & alarm = gather_task_set->alarms[sender_task_id][receiver_task_id];
            call_data->setToWaitingTunnelState();
            if likely (cq != nullptr)
            {
                alarm.Set(cq, Clock::now() + std::chrono::seconds(10), call_data->asGRPCKickTag());
            }
            return {nullptr, ""};
        }
        else
        {
            /// if call_data is already in WaitingTunnelState, then remove the alarm and return tunnel not found error
            if (gather_task_set != nullptr)
            {
                assert(query != nullptr);
                auto task_alarm_map_it = gather_task_set->alarms.find(sender_task_id);
                if (task_alarm_map_it != gather_task_set->alarms.end())
                {
                    task_alarm_map_it->second.erase(receiver_task_id);
                    if (task_alarm_map_it->second.empty())
                        gather_task_set->alarms.erase(task_alarm_map_it);
                }
                /// if the gather task set has no mpp task, it has to be removed if there is no alarms left,
                /// otherwise the gather task set itself may be left in MPPTaskManager forever
                if (gather_task_set->alarms.empty() && !gather_task_set->hasMPPTask())
                {
                    removeMPPGatherTaskSet(query, id.gather_id, false);
                    cv.notify_all();
                }
            }
            return {nullptr, fmt::format("{}: Can't find task [{}] within 10s.", req_info, id.toString())};
        }
    }
    /// don't need to delete the alarm here because registerMPPTask will delete all the related alarm

    return task->getTunnel(request);
}

std::pair<MPPTunnelPtr, String> MPPTaskManager::findTunnelWithTimeout(
    const ::mpp::EstablishMPPConnectionRequest * request,
    std::chrono::seconds timeout)
{
    const auto & meta = request->sender_meta();
    MPPTaskId id{meta};
    String req_info = fmt::format("tunnel{}+{}", request->sender_meta().task_id(), request->receiver_meta().task_id());
    MPPTask * task = nullptr;
    bool cancelled = false;
    String error_message;
    std::unique_lock lock(mu);
    auto ret = cv.wait_for(lock, timeout, [&] {
        auto [gather_task_set, error_msg] = getGatherTaskSetWithoutLock(id.gather_id);
        if (!error_msg.empty())
        {
            /// if the gather is aborted, return true to stop waiting timeout.
            LOG_WARNING(
                log,
                "{}: Gather <{}> is aborted, all its tasks are invalid.",
                req_info,
                id.gather_id.toString());
            cancelled = true;
            error_message = error_msg;
            return true;
        }
        if (gather_task_set == nullptr)
        {
            return false;
        }
        task = gather_task_set->findMPPTask(id);
        return task != nullptr;
    });
    fiu_do_on(FailPoints::random_task_manager_find_task_failure_failpoint, ret = false;);
    if (cancelled)
    {
        return {
            nullptr,
            fmt::format(
                "{}: Task [{},{}] has been aborted, error message: {}",
                req_info,
                meta.start_ts(),
                meta.task_id(),
                error_message)};
    }
    else if (!ret)
    {
        return {
            nullptr,
            fmt::format(
                "{}: Can't find task [{},{}] within {}s.",
                req_info,
                meta.start_ts(),
                meta.task_id(),
                timeout.count())};
    }
    return task->getTunnel(request);
}

void MPPTaskManager::abortMPPGather(const MPPGatherId & gather_id, const String & reason, AbortType abort_type)
{
    LOG_WARNING(
        log,
        "Begin to abort gather: {}, abort type: {}, reason: {}",
        gather_id.toString(),
        magic_enum::enum_name(abort_type),
        reason);
    MPPGatherTaskSetPtr gather_task_set;
    {
        /// abort task may take a long time, so first
        /// set a flag, so we can abort task one by
        /// one without holding the lock
        std::lock_guard lock(mu);
        aborted_query_gather_cache.add(gather_id, reason);
        auto [query, gather_task_set_local, _] = getMPPQueryAndGatherTaskSet(gather_id);
        if (query == nullptr || gather_task_set_local == nullptr)
        {
            LOG_WARNING(log, "{} does not found in task manager, skip abort", gather_id.toString());
            return;
        }
        gather_task_set = gather_task_set_local;
        if (!gather_task_set->isInNormalState())
        {
            LOG_WARNING(log, "{} already in abort process, skip abort", gather_id.toString());
            return;
        }
        gather_task_set->state = MPPGatherTaskSet::Aborting;
        gather_task_set->error_message = reason;
        /// cancel all the alarms
        for (auto & alarms_per_task : gather_task_set->alarms)
        {
            for (auto & alarm : alarms_per_task.second)
                alarm.second.Cancel();
        }
        gather_task_set->alarms.clear();
        if (!gather_task_set->hasMPPTask())
        {
            LOG_INFO(log, "There is no mpp task for {}, finish abort", gather_id.toString());
            removeMPPGatherTaskSet(query, gather_id, true);
            cv.notify_all();
            return;
        }
        scheduler->deleteQuery(gather_id.query_id, *this, true, gather_id.gather_id);
        cv.notify_all();
    }

    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("Remaining task in gather {} are: ", gather_id.toString());
    gather_task_set->forEachMPPTask(
        [&](const std::pair<MPPTaskId, MPPTaskPtr> & it) { fmt_buf.fmtAppend("{} ", it.first.toString()); });
    LOG_WARNING(log, fmt_buf.toString());

    gather_task_set->forEachMPPTask([&](const std::pair<MPPTaskId, MPPTaskPtr> & it) {
        if (it.second != nullptr)
            it.second->abort(reason, abort_type);
    });

    {
        std::lock_guard lock(mu);
        auto [query, gather, _] = getMPPQueryAndGatherTaskSet(gather_id);
        RUNTIME_ASSERT(
            gather != nullptr,
            log,
            "MPPGatherTaskSet {} should remaining in MPPTaskManager",
            gather_id.toString());
        gather->state = MPPGatherTaskSet::Aborted;
        cv.notify_all();
    }
    LOG_WARNING(log, "Finish abort gather: " + gather_id.toString());
}

std::pair<bool, String> MPPTaskManager::registerTask(MPPTask * task)
{
    if (!task->isRootMPPTask())
    {
        FAIL_POINT_PAUSE(FailPoints::pause_before_register_non_root_mpp_task);
    }
    std::unique_lock lock(mu);
    auto [query, gather_task_set, error_msg] = getMPPQueryAndGatherTaskSet(task->id.gather_id);
    if (!error_msg.empty())
    {
        return {false, fmt::format("gather is being aborted, error message = {}", error_msg)};
    }

    auto & context = task->context;

    if (query == nullptr)
        query = addMPPQuery(
            task->id.gather_id.query_id,
            task->id.gather_id.hasMeaningfulGatherId(),
            task->context->getSettingsRef().auto_spill_check_min_interval_ms);
    if (query->process_list_entry == nullptr)
    {
        query->process_list_entry = setProcessListElement(
            *context,
            context->getDAGContext()->dummy_query_string,
            context->getDAGContext()->dummy_ast.get(),
            true);
    }
    if (gather_task_set == nullptr)
    {
        gather_task_set = query->addMPPGatherTaskSet(task->id.gather_id);
    }
    if (gather_task_set->isTaskRegistered(task->id))
    {
        return {false, "task is already registered"};
    }
    gather_task_set->registerTask(task->id);
    task->is_registered = true;
    task->initProcessListEntry(query->process_list_entry);
    task->initQueryOperatorSpillContexts(query->mpp_query_operator_spill_contexts);
    return {true, ""};
}

MPPQueryId MPPTaskManager::getCurrentMinTSOQueryId(const String & resource_group_name)
{
    std::lock_guard lock(mu);
    return scheduler->getCurrentMinTSOQueryId(resource_group_name);
}

bool MPPTaskManager::isTaskExists(const MPPTaskId & id)
{
    std::unique_lock lock(mu);
    auto [query, gather_task_set, error_msg] = getMPPQueryAndGatherTaskSet(id.gather_id);
    if (gather_task_set == nullptr)
        return false;
    return gather_task_set->isTaskRegistered(id);
}

std::pair<bool, String> MPPTaskManager::makeTaskActive(MPPTaskPtr task)
{
    if (!task->isRootMPPTask())
    {
        FAIL_POINT_PAUSE(FailPoints::pause_before_make_non_root_mpp_task_active);
    }
    std::unique_lock lock(mu);
    auto [query, gather_task_set, error_msg] = getMPPQueryAndGatherTaskSet(task->id.gather_id);
    if (!error_msg.empty())
    {
        return {false, fmt::format("Gather is aborted, error message = {}", error_msg)};
    }
    /// gather_task_set must not be nullptr if the current query is not aborted since MPPTaskManager::registerTask
    /// always create the gather_task_set
    RUNTIME_CHECK_MSG(query != nullptr, "query must not be null when make task visible");
    RUNTIME_CHECK_MSG(gather_task_set != nullptr, "gather set must not be null when make task visible");
    if (gather_task_set->findMPPTask(task->id) != nullptr)
    {
        return {false, "task is already visible"};
    }
    RUNTIME_CHECK_MSG(
        query->process_list_entry.get() == task->process_list_entry_holder.process_list_entry.get(),
        "Task process list entry should always be the same as query process list entry");
    gather_task_set->makeTaskActive(task);
    gather_task_set->cancelAlarmsBySenderTaskId(task->id);
    cv.notify_all();
    return {true, ""};
}

std::pair<bool, String> MPPTaskManager::unregisterTask(const MPPTaskId & id, const String & error_message)
{
    std::unique_lock lock(mu);
    MPPGatherTaskSetPtr gather_task_set = nullptr;
    MPPQueryPtr query = nullptr;
    cv.wait(lock, [&] {
        String reason;
        std::tie(query, gather_task_set, reason) = getMPPQueryAndGatherTaskSet(id.gather_id);
        return gather_task_set == nullptr || gather_task_set->allowUnregisterTask();
    });
    if (gather_task_set != nullptr)
    {
        assert(query != nullptr);
        if (gather_task_set->isTaskRegistered(id))
        {
            gather_task_set->markTaskAsFinishedOrFailed(id, error_message);
            if (!gather_task_set->hasMPPTask() && gather_task_set->alarms.empty())
            {
                removeMPPGatherTaskSet(query, id.gather_id, false);
            }
            cv.notify_all();
            return {true, ""};
        }
    }
    cv.notify_all();
    return {false, "task can not be found, maybe not registered yet"};
}

String MPPTaskManager::toString()
{
    std::lock_guard lock(mu);
    String res("(");
    for (auto & query_it : mpp_query_map)
    {
        for (auto & gather_it : query_it.second->mpp_gathers)
        {
            gather_it.second->forEachMPPTask(
                [&](const std::pair<MPPTaskId, MPPTaskPtr> & it) { res += it.first.toString() + ", "; });
        }
    }
    return res + ")";
}

std::pair<MPPGatherTaskSetPtr, String> MPPTaskManager::getGatherTaskSetWithoutLock(const MPPGatherId & gather_id)
{
    auto [_, gather, aborted_reason] = getMPPQueryAndGatherTaskSet(gather_id);
    return {gather, aborted_reason};
}

std::tuple<MPPQueryPtr, MPPGatherTaskSetPtr, String> MPPTaskManager::getMPPQueryAndGatherTaskSet(
    const MPPGatherId & gather_id)
{
    auto reason = aborted_query_gather_cache.check(gather_id);
    const auto & query = getMPPQueryWithoutLock(gather_id.query_id);
    if (query == nullptr)
    {
        return std::make_tuple(nullptr, nullptr, reason);
    }
    RUNTIME_CHECK_MSG(
        query->has_meaningful_gather_id == gather_id.hasMeaningfulGatherId(),
        "MPP query has gather id while mpp task does not have gather id, should be something wrong in TiDB side");
    auto gather_it = query->mpp_gathers.find(gather_id);
    if (gather_it != query->mpp_gathers.end())
    {
        if (!gather_it->second->isInNormalState() && reason.empty())
            reason = gather_it->second->error_message;
        return std::make_tuple(query, gather_it->second, reason);
    }
    else
    {
        return std::make_tuple(query, nullptr, reason);
    }
}

std::pair<MPPGatherTaskSetPtr, String> MPPTaskManager::getGatherTaskSet(const MPPGatherId & gather_id)
{
    std::lock_guard lock(mu);
    return getGatherTaskSetWithoutLock(gather_id);
}

bool MPPTaskManager::tryToScheduleTask(MPPTaskScheduleEntry & schedule_entry)
{
    std::lock_guard lock(mu);
    return scheduler->tryToSchedule(schedule_entry, *this);
}

void MPPTaskManager::releaseThreadsFromScheduler(const String & resource_group_name, const int needed_threads)
{
    std::lock_guard lock(mu);
    scheduler->releaseThreadsThenSchedule(resource_group_name, needed_threads, *this);
}
} // namespace DB
