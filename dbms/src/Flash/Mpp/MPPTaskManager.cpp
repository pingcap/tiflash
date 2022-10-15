// Copyright 2022 PingCAP, Ltd.
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
#include <Flash/Mpp/MPPTaskManager.h>
#include <fmt/core.h>

#include <magic_enum.hpp>
#include <string>
#include <unordered_map>

namespace DB
{
namespace FailPoints
{
extern const char random_task_manager_find_task_failure_failpoint[];
} // namespace FailPoints

MPPTaskManager::MPPTaskManager(MPPTaskSchedulerPtr scheduler_)
    : scheduler(std::move(scheduler_))
    , log(Logger::get())
{}

std::pair<MPPTunnelPtr, String> MPPTaskManager::findTunnelWithTimeout(const ::mpp::EstablishMPPConnectionRequest * request, std::chrono::seconds timeout)
{
    const auto & meta = request->sender_meta();
    MPPTaskId id{meta.start_ts(), meta.task_id()};
    std::unordered_map<MPPTaskId, MPPTaskPtr>::iterator it;
    bool cancelled = false;
    String error_message;
    std::unique_lock lock(mu);
    auto ret = cv.wait_for(lock, timeout, [&] {
        auto query_it = mpp_query_map.find(id.start_ts);
        // TODO: how about the query has been cancelled in advance?
        if (query_it == mpp_query_map.end())
        {
            return false;
        }
        else if (!query_it->second->isInNormalState())
        {
            /// if the query is aborted, return true to stop waiting timeout.
            LOG_WARNING(log, fmt::format("Query {} is aborted, all its tasks are invalid.", id.start_ts));
            cancelled = true;
            error_message = query_it->second->error_message;
            return true;
        }
        it = query_it->second->task_map.find(id);
        return it != query_it->second->task_map.end();
    });
    fiu_do_on(FailPoints::random_task_manager_find_task_failure_failpoint, ret = false;);
    if (cancelled)
    {
        return {nullptr, fmt::format("Task [{},{}] has been aborted, error message: {}", meta.start_ts(), meta.task_id(), error_message)};
    }
    else if (!ret)
    {
        return {nullptr, fmt::format("Can't find task [{},{}] within {} s.", meta.start_ts(), meta.task_id(), timeout.count())};
    }
    return it->second->getTunnel(request);
}

void MPPTaskManager::abortMPPQuery(UInt64 query_id, const String & reason, AbortType abort_type)
{
    LOG_WARNING(log, fmt::format("Begin to abort query: {}, abort type: {}, reason: {}", query_id, magic_enum::enum_name(abort_type), reason));
    MPPQueryTaskSetPtr task_set;
    {
        /// abort task may take a long time, so first
        /// set a flag, so we can abort task one by
        /// one without holding the lock
        std::lock_guard lock(mu);
        auto it = mpp_query_map.find(query_id);
        if (it == mpp_query_map.end())
        {
            LOG_WARNING(log, fmt::format("{} does not found in task manager, skip abort", query_id));
            return;
        }
        else if (!it->second->isInNormalState())
        {
            LOG_WARNING(log, fmt::format("{} already in abort process, skip abort", query_id));
            return;
        }
        it->second->state = MPPQueryTaskSet::Aborting;
        it->second->error_message = reason;
        task_set = it->second;
        scheduler->deleteQuery(query_id, *this, true);
        cv.notify_all();
    }

    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("Remaining task in query {} are: ", query_id);
    for (auto & it : task_set->task_map)
        fmt_buf.fmtAppend("{} ", it.first.toString());
    LOG_WARNING(log, fmt_buf.toString());

    for (auto & it : task_set->task_map)
        it.second->abort(reason, abort_type);

    {
        std::lock_guard lock(mu);
        auto it = mpp_query_map.find(query_id);
        RUNTIME_ASSERT(it != mpp_query_map.end(), log, "MPPTaskQuerySet {} should remaining in MPPTaskManager", query_id);
        it->second->state = MPPQueryTaskSet::Aborted;
        cv.notify_all();
    }
    LOG_WARNING(log, "Finish abort query: " + std::to_string(query_id));
}

std::pair<bool, String> MPPTaskManager::registerTask(MPPTaskPtr task)
{
    std::unique_lock lock(mu);
    const auto & it = mpp_query_map.find(task->id.start_ts);
    if (it != mpp_query_map.end() && !it->second->isInNormalState())
    {
        return {false, fmt::format("query is being aborted, error message = {}", it->second->error_message)};
    }
    if (it != mpp_query_map.end() && it->second->task_map.find(task->id) != it->second->task_map.end())
    {
        return {false, "task has been registered"};
    }
    if (it == mpp_query_map.end()) /// the first one
    {
        auto ptr = std::make_shared<MPPQueryTaskSet>();
        ptr->task_map.emplace(task->id, task);
        mpp_query_map.insert({task->id.start_ts, ptr});
        GET_METRIC(tiflash_mpp_task_manager, type_mpp_query_count).Set(mpp_query_map.size());
    }
    else
    {
        mpp_query_map[task->id.start_ts]->task_map.emplace(task->id, task);
    }
    task->registered = true;
    cv.notify_all();
    return {true, ""};
}

std::pair<bool, String> MPPTaskManager::unregisterTask(const MPPTaskId & id)
{
    std::unique_lock lock(mu);
    auto it = mpp_query_map.end();
    cv.wait(lock, [&] {
        it = mpp_query_map.find(id.start_ts);
        return it == mpp_query_map.end() || it->second->allowUnregisterTask();
    });
    if (it != mpp_query_map.end())
    {
        auto task_it = it->second->task_map.find(id);
        if (task_it != it->second->task_map.end())
        {
            it->second->task_map.erase(task_it);
            if (it->second->task_map.empty())
            {
                /// remove query task map if the task is the last one
                scheduler->deleteQuery(id.start_ts, *this, false);
                mpp_query_map.erase(it);
                GET_METRIC(tiflash_mpp_task_manager, type_mpp_query_count).Set(mpp_query_map.size());
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
        for (auto & it : query_it.second->task_map)
            res += it.first.toString() + ", ";
    }
    return res + ")";
}

MPPQueryTaskSetPtr MPPTaskManager::getQueryTaskSetWithoutLock(UInt64 query_id)
{
    auto it = mpp_query_map.find(query_id);
    return it == mpp_query_map.end() ? nullptr : it->second;
}

MPPQueryTaskSetPtr MPPTaskManager::getQueryTaskSet(UInt64 query_id)
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
