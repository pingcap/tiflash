// Copyright 2023 PingCAP, Ltd.
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

MPPQueryTaskSet::~MPPQueryTaskSet()
{
    if likely (process_list_entry != nullptr)
    {
        auto peak_memory = process_list_entry->get().getMemoryTrackerPtr()->getPeak();
        GET_METRIC(tiflash_coprocessor_request_memory_usage, type_run_mpp_query).Observe(peak_memory);
    }
}

MPPTask * MPPQueryTaskSet::findMPPTask(const MPPTaskId & task_id) const
{
    const auto & it = task_map.find(task_id);
    if (it == task_map.end())
        return nullptr;
    return it->second.get();
}

MPPTaskManager::MPPTaskManager(MPPTaskSchedulerPtr scheduler_)
    : scheduler(std::move(scheduler_))
    , aborted_query_gather_cache(ABORTED_MPPGATHER_CACHE_SIZE)
    , log(Logger::get())
    , monitor(std::make_shared<MPPTaskMonitor>(log))
{}

MPPTaskManager::~MPPTaskManager()
{
    std::lock_guard lock(monitor->mu);
    monitor->is_shutdown = true;
    monitor->cv.notify_all();
}

MPPQueryTaskSetPtr MPPTaskManager::addMPPQueryTaskSet(const MPPQueryId & query_id)
{
    auto ptr = std::make_shared<MPPQueryTaskSet>();
    mpp_query_map.insert({query_id, ptr});
    GET_METRIC(tiflash_mpp_task_manager, type_mpp_query_count).Set(mpp_query_map.size());
    return ptr;
}

void MPPTaskManager::removeMPPQueryTaskSet(const MPPQueryId & query_id, bool on_abort)
{
    scheduler->deleteQuery(query_id, *this, on_abort);
    mpp_query_map.erase(query_id);
    GET_METRIC(tiflash_mpp_task_manager, type_mpp_query_count).Set(mpp_query_map.size());
}

std::pair<MPPTunnelPtr, String> MPPTaskManager::findAsyncTunnel(const ::mpp::EstablishMPPConnectionRequest * request, EstablishCallData * call_data, grpc::CompletionQueue * cq)
{
    const auto & meta = request->sender_meta();
    MPPTaskId id{meta};
    Int64 sender_task_id = meta.task_id();
    Int64 receiver_task_id = request->receiver_meta().task_id();
    String req_info = fmt::format("tunnel{}+{}", request->sender_meta().task_id(), request->receiver_meta().task_id());

    std::unique_lock lock(mu);
    auto [query_set, error_msg] = getQueryTaskSetWithoutLock(id.query_id);
    if (!error_msg.empty())
    {
        /// if the query is aborted, return the error message
        LOG_WARNING(log, fmt::format("{}: Query {} is aborted, all its tasks are invalid.", req_info, id.query_id.toString()));
        /// meet error
        return {nullptr, error_msg};
    }

    auto * task = query_set == nullptr ? nullptr : query_set->findMPPTask(id);
    if (task == nullptr)
    {
        /// task not found or not visible yet
        if (!call_data->isWaitingTunnelState())
        {
            /// if call_data is in new_request state, put it to waiting tunnel state
            if (query_set == nullptr)
                query_set = addMPPQueryTaskSet(id.query_id);
            auto & alarm = query_set->alarms[sender_task_id][receiver_task_id];
            call_data->setToWaitingTunnelState();
            alarm.Set(cq, Clock::now() + std::chrono::seconds(10), call_data);
            return {nullptr, ""};
        }
        else
        {
            /// if call_data is already in WaitingTunnelState, then remove the alarm and return tunnel not found error
            if (query_set != nullptr)
            {
                auto task_alarm_map_it = query_set->alarms.find(sender_task_id);
                if (task_alarm_map_it != query_set->alarms.end())
                {
                    task_alarm_map_it->second.erase(receiver_task_id);
                    if (task_alarm_map_it->second.empty())
                        query_set->alarms.erase(task_alarm_map_it);
                }
                if (query_set->alarms.empty() && !query_set->hasMPPTask())
                {
                    /// if the query task set has no mpp task, it has to be removed if there is no alarms left,
                    /// otherwise the query task set itself may be left in MPPTaskManager forever
                    removeMPPQueryTaskSet(id.query_id, false);
                    cv.notify_all();
                }
            }
            return {nullptr, fmt::format("{}: Can't find task [{}] within 10s.", req_info, id.toString())};
        }
    }
    /// don't need to delete the alarm here because registerMPPTask will delete all the related alarm

    return task->getTunnel(request);
}

std::pair<MPPTunnelPtr, String> MPPTaskManager::findTunnelWithTimeout(const ::mpp::EstablishMPPConnectionRequest * request, std::chrono::seconds timeout)
{
    const auto & meta = request->sender_meta();
    MPPTaskId id{meta};
    String req_info = fmt::format("tunnel{}+{}", request->sender_meta().task_id(), request->receiver_meta().task_id());
    MPPTask * task = nullptr;
    bool cancelled = false;
    String error_message;
    std::unique_lock lock(mu);
    auto ret = cv.wait_for(lock, timeout, [&] {
        auto [query_set, error_msg] = getQueryTaskSetWithoutLock(id.query_id);
        if (!error_msg.empty())
        {
            /// if the query is aborted, return true to stop waiting timeout.
            LOG_WARNING(log, fmt::format("{}: Query {} is aborted, all its tasks are invalid.", req_info, id.query_id.toString()));
            cancelled = true;
            error_message = error_msg;
            return true;
        }
        if (query_set == nullptr)
        {
            return false;
        }
        task = query_set->findMPPTask(id);
        return task != nullptr;
    });
    fiu_do_on(FailPoints::random_task_manager_find_task_failure_failpoint, ret = false;);
    if (cancelled)
    {
        return {nullptr, fmt::format("{}: Task [{},{}] has been aborted, error message: {}", req_info, meta.start_ts(), meta.task_id(), error_message)};
    }
    else if (!ret)
    {
        return {nullptr, fmt::format("{}: Can't find task [{},{}] within {}s.", req_info, meta.start_ts(), meta.task_id(), timeout.count())};
    }
    return task->getTunnel(request);
}

void MPPTaskManager::abortMPPQuery(const MPPQueryId & query_id, const String & reason, AbortType abort_type)
{
    LOG_WARNING(log, fmt::format("Begin to abort query: {}, abort type: {}, reason: {}", query_id.toString(), magic_enum::enum_name(abort_type), reason));
    MPPQueryTaskSetPtr task_set;
    {
        /// abort task may take a long time, so first
        /// set a flag, so we can abort task one by
        /// one without holding the lock
        std::lock_guard lock(mu);
        /// gather_id is not set by TiDB, so use 0 instead
        aborted_query_gather_cache.add(MPPGatherId(0, query_id), reason);
        auto it = mpp_query_map.find(query_id);
        if (it == mpp_query_map.end())
        {
            LOG_WARNING(log, fmt::format("{} does not found in task manager, skip abort", query_id.toString()));
            return;
        }
        else if (!it->second->isInNormalState())
        {
            LOG_WARNING(log, fmt::format("{} already in abort process, skip abort", query_id.toString()));
            return;
        }
        it->second->state = MPPQueryTaskSet::Aborting;
        it->second->error_message = reason;
        /// cancel all the alarms
        for (auto & alarms_per_task : it->second->alarms)
        {
            for (auto & alarm : alarms_per_task.second)
                alarm.second.Cancel();
        }
        it->second->alarms.clear();
        if (!it->second->hasMPPTask())
        {
            LOG_INFO(log, fmt::format("There is no mpp task for {}, finish abort", query_id.toString()));
            removeMPPQueryTaskSet(query_id, true);
            cv.notify_all();
            return;
        }
        task_set = it->second;
        scheduler->deleteQuery(query_id, *this, true);
        cv.notify_all();
    }

    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("Remaining task in query {} are: ", query_id.toString());
    task_set->forEachMPPTask([&](const std::pair<MPPTaskId, MPPTaskPtr> & it) {
        fmt_buf.fmtAppend("{} ", it.first.toString());
    });
    LOG_WARNING(log, fmt_buf.toString());

    task_set->forEachMPPTask([&](const std::pair<MPPTaskId, MPPTaskPtr> & it) {
        if (it.second != nullptr)
            it.second->abort(reason, abort_type);
    });

    {
        std::lock_guard lock(mu);
        auto it = mpp_query_map.find(query_id);
        RUNTIME_ASSERT(it != mpp_query_map.end(), log, "MPPTaskQuerySet {} should remaining in MPPTaskManager", query_id.toString());
        it->second->state = MPPQueryTaskSet::Aborted;
        cv.notify_all();
    }
    LOG_WARNING(log, "Finish abort query: " + query_id.toString());
}

std::pair<bool, String> MPPTaskManager::registerTask(MPPTask * task)
{
    if (!task->isRootMPPTask())
    {
        FAIL_POINT_PAUSE(FailPoints::pause_before_register_non_root_mpp_task);
    }
    std::unique_lock lock(mu);
    auto [query_set, error_msg] = getQueryTaskSetWithoutLock(task->id.query_id);
    if (!error_msg.empty())
    {
        return {false, fmt::format("query is being aborted, error message = {}", error_msg)};
    }

    auto & context = task->context;

    if (query_set == nullptr)
        query_set = addMPPQueryTaskSet(task->id.query_id);
    if (query_set->process_list_entry == nullptr)
    {
        query_set->process_list_entry = setProcessListElement(
            *context,
            context->getDAGContext()->dummy_query_string,
            context->getDAGContext()->dummy_ast.get(),
            true);
    }
    if (query_set->isTaskRegistered(task->id))
    {
        return {false, "task is already registered"};
    }
    query_set->registerTask(task->id);
    task->initProcessListEntry(query_set->process_list_entry);
    return {true, ""};
}

std::pair<bool, String> MPPTaskManager::makeTaskActive(MPPTaskPtr task)
{
    if (!task->isRootMPPTask())
    {
        FAIL_POINT_PAUSE(FailPoints::pause_before_make_non_root_mpp_task_active);
    }
    std::unique_lock lock(mu);
    auto [query_set, error_msg] = getQueryTaskSetWithoutLock(task->id.query_id);
    if (!error_msg.empty())
    {
        return {false, fmt::format("query is being aborted, error message = {}", error_msg)};
    }
    if (query_set->findMPPTask(task->id) != nullptr)
    {
        return {false, "task is already visible"};
    }
    /// query_set must not be nullptr if the current query is not aborted since MPPTaskManager::registerTask
    /// always create the query_set
    RUNTIME_CHECK_MSG(query_set != nullptr, "query set must not be null when make task visible");
    RUNTIME_CHECK_MSG(query_set->process_list_entry.get() == task->process_list_entry_holder.process_list_entry.get(),
                      "Task process list entry should always be the same as query process list entry");
    query_set->makeTaskActive(task);
    /// cancel all the alarm waiting on this task
    auto alarm_it = query_set->alarms.find(task->id.task_id);
    if (alarm_it != query_set->alarms.end())
    {
        for (auto & alarm : alarm_it->second)
            alarm.second.Cancel();
        query_set->alarms.erase(alarm_it);
    }
    task->is_public = true;
    cv.notify_all();
    return {true, ""};
}

std::pair<bool, String> MPPTaskManager::unregisterTask(const MPPTaskId & id)
{
    std::unique_lock lock(mu);
    auto it = mpp_query_map.end();
    cv.wait(lock, [&] {
        it = mpp_query_map.find(id.query_id);
        return it == mpp_query_map.end() || it->second->allowUnregisterTask();
    });
    if (it != mpp_query_map.end())
    {
        if (it->second->isTaskRegistered(id))
        {
            it->second->removeMPPTask(id);
            if (!it->second->hasMPPTask() && it->second->alarms.empty())
                removeMPPQueryTaskSet(id.query_id, false);
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
        query_it.second->forEachMPPTask([&](const std::pair<MPPTaskId, MPPTaskPtr> & it) {
            res += it.first.toString() + ", ";
        });
    }
    return res + ")";
}

std::pair<MPPQueryTaskSetPtr, String> MPPTaskManager::getQueryTaskSetWithoutLock(const MPPQueryId & query_id)
{
    auto it = mpp_query_map.find(query_id);
    /// gather_id is not set by TiDB, so use 0 instead
    auto reason = aborted_query_gather_cache.check(MPPGatherId(0, query_id));
    if (it != mpp_query_map.end())
    {
        if (!it->second->isInNormalState() && reason.empty())
            reason = it->second->error_message;
        return std::make_tuple(it->second, reason);
    }
    else
    {
        return std::make_tuple(nullptr, reason);
    }
}

std::pair<MPPQueryTaskSetPtr, String> MPPTaskManager::getQueryTaskSet(const MPPQueryId & query_id)
{
    std::lock_guard lock(mu);
    return getQueryTaskSetWithoutLock(query_id);
}

bool MPPTaskManager::tryToScheduleTask(MPPTaskScheduleEntry & schedule_entry)
{
    std::lock_guard lock(mu);
    return scheduler->tryToSchedule(schedule_entry, *this);
}

void MPPTaskManager::releaseThreadsFromScheduler(const int needed_threads)
{
    std::lock_guard lock(mu);
    scheduler->releaseThreadsThenSchedule(needed_threads, *this);
}
} // namespace DB
