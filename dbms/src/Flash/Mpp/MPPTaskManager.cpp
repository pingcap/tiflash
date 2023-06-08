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

#include <Common/FmtUtils.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <fmt/core.h>

#include <string>
#include <thread>
#include <unordered_map>

namespace DB
{
<<<<<<< HEAD
MPPTaskManager::MPPTaskManager(MPPTaskSchedulerPtr scheduler_)
    : scheduler(std::move(scheduler_))
    , log(&Poco::Logger::get("TaskManager"))
=======
namespace FailPoints
{
extern const char random_task_manager_find_task_failure_failpoint[];
extern const char pause_before_register_non_root_mpp_task[];
} // namespace FailPoints

MPPTaskManager::MPPTaskManager(MPPTaskSchedulerPtr scheduler_)
    : scheduler(std::move(scheduler_))
    , aborted_query_gather_cache(ABORTED_MPPGATHER_CACHE_SIZE)
    , log(Logger::get())
    , monitor(std::make_shared<MPPTaskMonitor>(log))
>>>>>>> 306d6b785e (Fix unstable tests and add more ut (#7613))
{}

MPPTaskPtr MPPTaskManager::findTaskWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout, std::string & errMsg)
{
<<<<<<< HEAD
    MPPTaskId id{meta.start_ts(), meta.task_id()};
=======
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

    if (query_set == nullptr || query_set->task_map.find(id) == query_set->task_map.end())
    {
        /// task not found
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
                if (query_set->alarms.empty() && query_set->task_map.empty())
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

    auto it = query_set->task_map.find(id);
    return it->second->getTunnel(request);
}

std::pair<MPPTunnelPtr, String> MPPTaskManager::findTunnelWithTimeout(const ::mpp::EstablishMPPConnectionRequest * request, std::chrono::seconds timeout)
{
    const auto & meta = request->sender_meta();
    MPPTaskId id{meta};
    String req_info = fmt::format("tunnel{}+{}", request->sender_meta().task_id(), request->receiver_meta().task_id());
>>>>>>> 306d6b785e (Fix unstable tests and add more ut (#7613))
    std::unordered_map<MPPTaskId, MPPTaskPtr>::iterator it;
    bool cancelled = false;
    std::unique_lock lock(mu);
    auto ret = cv.wait_for(lock, timeout, [&] {
<<<<<<< HEAD
        auto query_it = mpp_query_map.find(id.start_ts);
        // TODO: how about the query has been cancelled in advance?
        if (query_it == mpp_query_map.end())
=======
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
>>>>>>> 306d6b785e (Fix unstable tests and add more ut (#7613))
        {
            return false;
        }
        else if (query_it->second->to_be_cancelled)
        {
            /// if the query is cancelled, return true to stop waiting timeout.
            LOG_WARNING(log, fmt::format("Query {} is cancelled, all its tasks are invalid.", id.start_ts));
            cancelled = true;
            return true;
        }
        it = query_it->second->task_map.find(id);
        return it != query_it->second->task_map.end();
    });
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

void MPPTaskManager::cancelMPPQuery(UInt64 query_id, const String & reason)
{
    MPPQueryTaskSetPtr task_set;
    {
        /// cancel task may take a long time, so first
        /// set a flag, so we can cancel task one by
        /// one without holding the lock
        std::lock_guard lock(mu);
<<<<<<< HEAD
=======
        /// gather_id is not set by TiDB, so use 0 instead
        aborted_query_gather_cache.add(MPPGatherId(0, query_id), reason);
>>>>>>> 306d6b785e (Fix unstable tests and add more ut (#7613))
        auto it = mpp_query_map.find(query_id);
        if (it == mpp_query_map.end() || it->second->to_be_cancelled)
            return;
        it->second->to_be_cancelled = true;
        task_set = it->second;
        scheduler->deleteQuery(query_id, *this, true);
        cv.notify_all();
    }
    LOG_WARNING(log, fmt::format("Begin cancel query: {}", query_id));
    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("Remaining task in query {} are: ", query_id);
    // TODO: cancel tasks in order rather than issuing so many threads to cancel tasks
    std::vector<std::thread> cancel_workers;
    for (const auto & task : task_set->task_map)
    {
        fmt_buf.fmtAppend("{} ", task.first.toString());
        std::thread t(&MPPTask::cancel, task.second, std::ref(reason));
        cancel_workers.push_back(std::move(t));
    }
    LOG_WARNING(log, fmt_buf.toString());
    for (auto & worker : cancel_workers)
    {
        worker.join();
    }
    MPPQueryTaskSetPtr canceled_task_set;
    {
        std::lock_guard lock(mu);
        /// just to double check the query still exists
        auto it = mpp_query_map.find(query_id);
        if (it != mpp_query_map.end())
        {
            /// hold the canceled task set, so the mpp task will not be deconstruct when holding the
            /// `mu` of MPPTaskManager, otherwise it might cause deadlock
            canceled_task_set = it->second;
            mpp_query_map.erase(it);
        }
    }
    LOG_WARNING(log, "Finish cancel query: " + std::to_string(query_id));
}

bool MPPTaskManager::registerTask(MPPTaskPtr task)
{
    std::unique_lock lock(mu);
    const auto & it = mpp_query_map.find(task->id.start_ts);
    if (it != mpp_query_map.end() && it->second->to_be_cancelled)
    {
        LOG_WARNING(log, "Do not register task: " + task->id.toString() + " because the query is to be cancelled.");
        cv.notify_all();
        return false;
    }
    if (it != mpp_query_map.end() && it->second->task_map.find(task->id) != it->second->task_map.end())
    {
        throw Exception("The task " + task->id.toString() + " has been registered");
    }
<<<<<<< HEAD
    if (it == mpp_query_map.end()) /// the first one
    {
        auto ptr = std::make_shared<MPPQueryTaskSet>();
        ptr->task_map.emplace(task->id, task);
        mpp_query_map.insert({task->id.start_ts, ptr});
=======
    std::unique_lock lock(mu);
    auto [query_set, error_msg] = getQueryTaskSetWithoutLock(task->id.query_id);
    if (!error_msg.empty())
    {
        return {false, fmt::format("query is being aborted, error message = {}", error_msg)};
>>>>>>> 306d6b785e (Fix unstable tests and add more ut (#7613))
    }
    else
    {
        mpp_query_map[task->id.start_ts]->task_map.emplace(task->id, task);
    }
    task->manager = this;
    cv.notify_all();
    return true;
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
        if (task_it != it->second->task_map.end())
        {
            it->second->task_map.erase(task_it);
            if (it->second->task_map.empty())
            {
                /// remove query task map if the task is the last one
                scheduler->deleteQuery(task->id.start_ts, *this, false);
                mpp_query_map.erase(it);
            }
            return;
        }
    }
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

<<<<<<< HEAD
MPPQueryTaskSetPtr MPPTaskManager::getQueryTaskSetWithoutLock(UInt64 query_id)
{
    auto it = mpp_query_map.find(query_id);
    return it == mpp_query_map.end() ? nullptr : it->second;
}

bool MPPTaskManager::tryToScheduleTask(const MPPTaskPtr & task)
=======
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
>>>>>>> 306d6b785e (Fix unstable tests and add more ut (#7613))
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
