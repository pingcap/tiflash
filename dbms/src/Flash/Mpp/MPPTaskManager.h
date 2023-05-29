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

#pragma once

#include <Flash/EstablishCall.h>
#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/MinTSOScheduler.h>
#include <common/logger_useful.h>
#include <grpcpp/alarm.h>
#include <kvproto/mpp.pb.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{
struct MPPQueryTaskSet
{
    enum State
    {
        Normal,
        Aborting,
        Aborted,
    };
    /// task can only be registered state is Normal
    State state = Normal;
    String error_message;
    MPPTaskMap task_map;
    std::unordered_map<Int64, std::unordered_map<Int64, grpc::Alarm>> alarms;
    /// only used in scheduler
    std::queue<MPPTaskId> waiting_tasks;
    bool isInNormalState() const
    {
        return state == Normal;
    }
    bool allowUnregisterTask() const
    {
        return state == Normal || state == Aborted;
    }
};

using MPPQueryTaskSetPtr = std::shared_ptr<MPPQueryTaskSet>;

/// a map from the mpp query id to mpp query task set, we use
/// the query_ts + local_query_id + serverID as the query id, because TiDB can't guarantee
/// the uniqueness of the start ts when stale read or set snapshot
using MPPQueryMap = std::unordered_map<MPPQueryId, MPPQueryTaskSetPtr, MPPQueryIdHash>;

struct MPPTaskMonitor
{
public:
    explicit MPPTaskMonitor(const LoggerPtr & log_)
        : log(log_)
    {}

    void addMonitoredTask(const String & task_unique_id)
    {
        std::lock_guard lock(mu);
        auto iter = monitored_tasks.find(task_unique_id);
        if (iter != monitored_tasks.end())
        {
            LOG_WARNING(log, "task {} is repeatedly added to be monitored which is not an expected behavior!");
            return;
        }

        monitored_tasks.insert(std::make_pair(task_unique_id, Stopwatch()));
    }

    void removeMonitoredTask(const String & task_unique_id)
    {
        std::lock_guard lock(mu);
        auto iter = monitored_tasks.find(task_unique_id);
        if (iter == monitored_tasks.end())
        {
            LOG_WARNING(log, "Unexpected behavior! task {} is not found in monitored_task.");
            return;
        }

        monitored_tasks.erase(iter);
    }

    std::mutex mu;
    std::condition_variable cv;
    bool is_shutdown = false;
    const LoggerPtr log;

    // All created MPPTasks should be put into this variable.
    // Only when the MPPTask is completed destructed, the task can be removed from it.
    std::unordered_map<String, Stopwatch> monitored_tasks;
};

// MPPTaskManger holds all running mpp tasks. It's a single instance holden in Context.
class MPPTaskManager : public std::enable_shared_from_this<MPPTaskManager>
    , private boost::noncopyable
{
    MPPTaskSchedulerPtr scheduler;

    std::mutex mu;

    MPPQueryMap mpp_query_map;

    LoggerPtr log;

    std::condition_variable cv;

    std::shared_ptr<MPPTaskMonitor> monitor;

public:
    explicit MPPTaskManager(MPPTaskSchedulerPtr scheduler);

    ~MPPTaskManager();

    std::shared_ptr<MPPTaskMonitor> getMPPTaskMonitor() const { return monitor; }

    void addMonitoredTask(const String & task_unique_id) { monitor->addMonitoredTask(task_unique_id); }

    void removeMonitoredTask(const String & task_unique_id) { monitor->removeMonitoredTask(task_unique_id); }

    MPPQueryTaskSetPtr getQueryTaskSetWithoutLock(const MPPQueryId & query_id);

    MPPQueryTaskSetPtr getQueryTaskSet(const MPPQueryId & query_id);

    std::pair<bool, String> registerTask(MPPTaskPtr task);

    std::pair<bool, String> unregisterTask(const MPPTaskId & id);

    bool tryToScheduleTask(MPPTaskScheduleEntry & schedule_entry);

    void releaseThreadsFromScheduler(int needed_threads);

    std::pair<MPPTunnelPtr, String> findTunnelWithTimeout(const ::mpp::EstablishMPPConnectionRequest * request, std::chrono::seconds timeout);

    std::pair<MPPTunnelPtr, String> findAsyncTunnel(const ::mpp::EstablishMPPConnectionRequest * request, EstablishCallData * call_data, grpc::CompletionQueue * cq);

    void abortMPPQuery(const MPPQueryId & query_id, const String & reason, AbortType abort_type);

    String toString();

    // We can't start this thread in constructor as we inherit the `enable_shared_from_this`
    void startMonitorMPPTaskThread();

private:
    MPPQueryTaskSetPtr addMPPQueryTaskSet(const MPPQueryId & query_id);
    void removeMPPQueryTaskSet(const MPPQueryId & query_id, bool on_abort);
    void monitorMPPTasks();
};

} // namespace DB
