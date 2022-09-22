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
#include <Flash/Mpp/MPPTaskManager.h>
#include <fmt/core.h>

#include <string>
#include <thread>
#include <unordered_map>

namespace DB
{
namespace FailPoints
{
extern const char random_task_manager_find_task_failure_failpoint[];
} // namespace FailPoints

MPPTaskManager::MPPTaskManager(MPPTaskSchedulerPtr scheduler_)
    : scheduler(std::move(scheduler_))
    , log(&Poco::Logger::get("TaskManager"))
{}

MPPTaskPtr MPPTaskManager::findTaskWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout, std::string & errMsg)
{
    MPPTaskId id{meta.start_ts(), meta.task_id()};
    std::unordered_map<MPPTaskId, MPPTaskPtr>::iterator it;
    bool cancelled = false;
    std::unique_lock lock(mu);
    auto ret = cv.wait_for(lock, timeout, [&] {
        auto query_it = mpp_query_map.find(id.start_ts);
        // TODO: how about the query has been cancelled in advance?
        if (query_it == mpp_query_map.end())
        {
            return false;
        }
<<<<<<< HEAD
        else if (query_it->second->to_be_cancelled)
=======
        else if (!query_it->second->isInNormalState())
>>>>>>> 988cde9cfa (Do not use extra threads when cancel mpp query (#5966))
        {
            /// if the query is cancelled, return true to stop waiting timeout.
            LOG_WARNING(log, fmt::format("Query {} is cancelled, all its tasks are invalid.", id.start_ts));
            cancelled = true;
            return true;
        }
        it = query_it->second->task_map.find(id);
        return it != query_it->second->task_map.end();
    });
    fiu_do_on(FailPoints::random_task_manager_find_task_failure_failpoint, ret = false;);
    if (cancelled)
    {
        errMsg = fmt::format("Task [{},{}] has been cancelled.", meta.start_ts(), meta.task_id());
        return nullptr;
    }
    else if (!ret)
    {
        errMsg = fmt::format("Can't find task [{},{}] within {} s.", meta.start_ts(), meta.task_id(), timeout.count());
        return nullptr;
    }
    return it->second;
}

<<<<<<< HEAD
void MPPTaskManager::cancelMPPQuery(UInt64 query_id, const String & reason)
{
=======
void MPPTaskManager::abortMPPQuery(UInt64 query_id, const String & reason, AbortType abort_type)
{
    LOG_WARNING(log, fmt::format("Begin to abort query: {}, abort type: {}, reason: {}", query_id, magic_enum::enum_name(abort_type), reason));
>>>>>>> 988cde9cfa (Do not use extra threads when cancel mpp query (#5966))
    MPPQueryTaskSetPtr task_set;
    {
        /// cancel task may take a long time, so first
        /// set a flag, so we can cancel task one by
        /// one without holding the lock
        std::lock_guard lock(mu);
        auto it = mpp_query_map.find(query_id);
        if (it == mpp_query_map.end())
        {
            LOG_WARNING(log, fmt::format("{} does not found in task manager, skip cancel", query_id));
            return;
        }
<<<<<<< HEAD
        else if (it->second->to_be_cancelled)
=======
        else if (!it->second->isInNormalState())
>>>>>>> 988cde9cfa (Do not use extra threads when cancel mpp query (#5966))
        {
            LOG_WARNING(log, fmt::format("{} already in cancel process, skip cancel", query_id));
            return;
        }
<<<<<<< HEAD
        it->second->to_be_cancelled = true;
=======
        it->second->state = MPPQueryTaskSet::Aborting;
        it->second->error_message = reason;
>>>>>>> 988cde9cfa (Do not use extra threads when cancel mpp query (#5966))
        task_set = it->second;
        scheduler->deleteQuery(query_id, *this, true);
        cv.notify_all();
    }
<<<<<<< HEAD
    LOG_WARNING(log, fmt::format("Begin cancel query: {}", query_id));
    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("Remaining task in query {} are: ", query_id);
    // TODO: cancel tasks in order rather than issuing so many threads to cancel tasks
    auto thread_manager = newThreadManager();
    for (auto it = task_set->task_map.begin(); it != task_set->task_map.end();)
    {
        fmt_buf.fmtAppend("{} ", it->first.toString());
        auto current_task = it->second;
        it = task_set->task_map.erase(it);
        thread_manager->schedule(false, "CancelMPPTask", [task = std::move(current_task), &reason] { task->cancel(reason); });
    }
=======

    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("Remaining task in query {} are: ", query_id);
    for (auto & it : task_set->task_map)
        fmt_buf.fmtAppend("{} ", it.first.toString());
>>>>>>> 988cde9cfa (Do not use extra threads when cancel mpp query (#5966))
    LOG_WARNING(log, fmt_buf.toString());

    for (auto & it : task_set->task_map)
        it.second->abort(reason, abort_type);

    {
        std::lock_guard lock(mu);
        auto it = mpp_query_map.find(query_id);
<<<<<<< HEAD
        /// just to double check the query still exists
        if (it != mpp_query_map.end())
            mpp_query_map.erase(it);
=======
        RUNTIME_ASSERT(it != mpp_query_map.end(), log, "MPPTaskQuerySet {} should remaining in MPPTaskManager", query_id);
        it->second->state = MPPQueryTaskSet::Aborted;
        cv.notify_all();
>>>>>>> 988cde9cfa (Do not use extra threads when cancel mpp query (#5966))
    }
    LOG_WARNING(log, "Finish cancel query: " + std::to_string(query_id));
}

bool MPPTaskManager::registerTask(MPPTaskPtr task)
{
    std::unique_lock lock(mu);
    const auto & it = mpp_query_map.find(task->id.start_ts);
<<<<<<< HEAD
    if (it != mpp_query_map.end() && it->second->to_be_cancelled)
    {
        LOG_WARNING(log, "Do not register task: " + task->id.toString() + " because the query is to be cancelled.");
        cv.notify_all();
        return false;
=======
    if (it != mpp_query_map.end() && !it->second->isInNormalState())
    {
        return {false, fmt::format("query is being aborted, error message = {}", it->second->error_message)};
>>>>>>> 988cde9cfa (Do not use extra threads when cancel mpp query (#5966))
    }
    if (it != mpp_query_map.end() && it->second->task_map.find(task->id) != it->second->task_map.end())
    {
        throw Exception("The task " + task->id.toString() + " has been registered");
    }
    if (it == mpp_query_map.end()) /// the first one
    {
        auto ptr = std::make_shared<MPPQueryTaskSet>();
        ptr->task_map.emplace(task->id, task);
        mpp_query_map.insert({task->id.start_ts, ptr});
    }
    else
    {
        mpp_query_map[task->id.start_ts]->task_map.emplace(task->id, task);
    }
    task->registered = true;
    cv.notify_all();
    return true;
}

<<<<<<< HEAD
bool MPPTaskManager::isQueryToBeCancelled(UInt64 query_id)
{
    std::unique_lock lock(mu);
    auto it = mpp_query_map.find(query_id);
    return it != mpp_query_map.end() && it->second->to_be_cancelled;
}

void MPPTaskManager::unregisterTask(MPPTask * task)
{
    std::unique_lock lock(mu);
    auto it = mpp_query_map.find(task->id.start_ts);
    if (it != mpp_query_map.end())
    {
        if (it->second->to_be_cancelled)
            return;
        auto task_it = it->second->task_map.find(task->id);
=======
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
>>>>>>> 988cde9cfa (Do not use extra threads when cancel mpp query (#5966))
        if (task_it != it->second->task_map.end())
        {
            it->second->task_map.erase(task_it);
            if (it->second->task_map.empty())
            {
                /// remove query task map if the task is the last one
                scheduler->deleteQuery(id.start_ts, *this, false);
                mpp_query_map.erase(it);
            }
            return;
        }
    }
<<<<<<< HEAD
    LOG_ERROR(log, "The task " + task->id.toString() + " cannot be found and fail to unregister");
}

std::vector<UInt64> MPPTaskManager::getCurrentQueries()
{
    std::vector<UInt64> ret;
    std::lock_guard lock(mu);
    for (auto & it : mpp_query_map)
    {
        ret.push_back(it.first);
    }
    return ret;
}

std::vector<MPPTaskPtr> MPPTaskManager::getCurrentTasksForQuery(UInt64 query_id)
{
    std::vector<MPPTaskPtr> ret;
    std::lock_guard lock(mu);
    const auto & it = mpp_query_map.find(query_id);
    if (it == mpp_query_map.end() || it->second->to_be_cancelled)
        return ret;
    for (const auto & task_it : it->second->task_map)
        ret.push_back(task_it.second);
    return ret;
=======
    cv.notify_all();
    return {false, "task can not be found, maybe not registered yet"};
>>>>>>> 988cde9cfa (Do not use extra threads when cancel mpp query (#5966))
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

bool MPPTaskManager::tryToScheduleTask(const MPPTaskPtr & task)
{
    std::lock_guard lock(mu);
    return scheduler->tryToSchedule(task, *this);
}

void MPPTaskManager::releaseThreadsFromScheduler(const int needed_threads)
{
    std::lock_guard lock(mu);
    scheduler->releaseThreadsThenSchedule(needed_threads, *this);
}

} // namespace DB
