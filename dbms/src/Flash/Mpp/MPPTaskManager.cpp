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

std::pair<MPPTunnelPtr, String> MPPTaskManager::findTunnelWithTimeout(const ::mpp::EstablishMPPConnectionRequest * request, std::chrono::seconds timeout)
{
    const auto & meta = request->sender_meta();
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
    fiu_do_on(FailPoints::random_task_manager_find_task_failure_failpoint, ret = false;);
    if (cancelled)
    {
        return {nullptr, fmt::format("Task [{},{}] has been cancelled.", meta.start_ts(), meta.task_id())};
    }
    else if (!ret)
    {
        return {nullptr, fmt::format("Can't find task [{},{}] within {} s.", meta.start_ts(), meta.task_id(), timeout.count())};
    }
    return it->second->getTunnel(request);
}

class MPPTaskCancelHelper
{
public:
    MPPTaskPtr task;
    String reason;
    MPPTaskCancelHelper(MPPTaskPtr && task_, const String & reason_)
        : task(std::move(task_))
        , reason(reason_)
    {}
    DISALLOW_COPY_AND_MOVE(MPPTaskCancelHelper);
    void run() const
    {
        task->cancel(reason);
    }
};

void MPPTaskManager::cancelMPPQuery(UInt64 query_id, const String & reason)
{
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
        else if (it->second->to_be_cancelled)
        {
            LOG_WARNING(log, fmt::format("{} already in cancel process, skip cancel", query_id));
            return;
        }
        it->second->to_be_cancelled = true;
        task_set = it->second;
        scheduler->deleteQuery(query_id, *this, true);
        cv.notify_all();
    }
    LOG_WARNING(log, fmt::format("Begin cancel query: {}", query_id));
    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("Remaining task in query {} are: ", query_id);
    // TODO: cancel tasks in order rather than issuing so many threads to cancel tasks
    auto thread_manager = newThreadManager();
    try
    {
        for (auto it = task_set->task_map.begin(); it != task_set->task_map.end();)
        {
            fmt_buf.fmtAppend("{} ", it->first.toString());
            auto current_task = it->second;
            it = task_set->task_map.erase(it);
            // Note it is not acceptable to destruct `current_task` inside the loop, because destruct a mpp task before all
            // other mpp tasks are cancelled may cause some deadlock issues, so `current_task` has to be moved to cancel thread.
            // At first, we use std::move to move `current_task` to lambda like this:
            // thread_manager->schedule(false, "CancelMPPTask", [task = std::move(current_task), &reason] { task->cancel(reason); });
            // However, due to SOO in llvm(https://github.com/llvm/llvm-project/issues/32472), there is still a copy of `current_task`
            // remaining in the current scope, as a workaround we add a wrap(MPPTaskCancelHelper) here to make sure `current_task`
            // can be moved to cancel thread.
            thread_manager->schedule(false, "CancelMPPTask", [helper = new MPPTaskCancelHelper(std::move(current_task), reason)] {
                std::unique_ptr<MPPTaskCancelHelper>(helper)->run();
            });
        }
    }
    catch (...)
    {
        thread_manager->wait();
        throw;
    }
    LOG_WARNING(log, fmt_buf.toString());
    thread_manager->wait();
    {
        std::lock_guard lock(mu);
        auto it = mpp_query_map.find(query_id);
        /// just to double check the query still exists
        if (it != mpp_query_map.end())
            mpp_query_map.erase(it);
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
    task->manager = this;
    cv.notify_all();
    return true;
}

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
