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

#pragma once

#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/MinTSOScheduler.h>
#include <common/logger_useful.h>
#include <kvproto/mpp.pb.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

namespace DB
{
struct MPPQueryTaskSet
{
    /// to_be_cancelled is kind of lock, if to_be_cancelled is set
    /// to true, then task_map can only be modified by query cancel
    /// thread, which means no task can register/un-register for the
    /// query, here we do not need mutex because all the write/read
    /// to MPPQueryTaskSet is protected by the mutex in MPPTaskManager
    bool to_be_cancelled = false;
    MPPTaskMap task_map;
    /// only used in scheduler
    std::queue<MPPTaskId> waiting_tasks;
};

using MPPQueryTaskSetPtr = std::shared_ptr<MPPQueryTaskSet>;

<<<<<<< HEAD
/// a map from the mpp query id to mpp query task set, we use
/// the start ts of a query as the query id as TiDB will guarantee
/// the uniqueness of the start ts
using MPPQueryMap = std::unordered_map<UInt64, MPPQueryTaskSetPtr>;
=======
struct MPPTaskMonitor
{
public:
    explicit MPPTaskMonitor(const LoggerPtr & log_)
        : log(log_)
    {}

    bool addMonitoredTask(const String & task_unique_id)
    {
        std::lock_guard lock(mu);
        auto iter = monitored_tasks.find(task_unique_id);
        if (iter != monitored_tasks.end())
        {
            LOG_WARNING(
                log,
                "task {} is repeatedly added to be monitored which is not an expected behavior!",
                task_unique_id);
            return false;
        }

        monitored_tasks.insert(std::make_pair(task_unique_id, Stopwatch()));
        return true;
    }

    void removeMonitoredTask(const String & task_unique_id)
    {
        std::lock_guard lock(mu);
        auto iter = monitored_tasks.find(task_unique_id);
        if (iter == monitored_tasks.end())
        {
            LOG_WARNING(log, "Unexpected behavior! task {} is not found in monitored_task.", task_unique_id);
            return;
        }

        monitored_tasks.erase(iter);
    }

    bool isInMonitor(const String & task_unique_id)
    {
        std::lock_guard lock(mu);
        return monitored_tasks.find(task_unique_id) != monitored_tasks.end();
    }

    std::mutex mu;
    std::condition_variable cv;
    bool is_shutdown = false;
    const LoggerPtr log;

    // All created MPPTasks should be put into this variable.
    // Only when the MPPTask is completed destructed, the task can be removed from it.
    std::unordered_map<String, Stopwatch> monitored_tasks;
};
>>>>>>> 96a006956b (Fix potential hang when duplicated task registered. (#8193))

// MPPTaskManger holds all running mpp tasks. It's a single instance holden in Context.
class MPPTaskManager : private boost::noncopyable
{
    MPPTaskSchedulerPtr scheduler;

    std::mutex mu;

    MPPQueryMap mpp_query_map;

    Poco::Logger * log;

    std::condition_variable cv;

public:
    explicit MPPTaskManager(MPPTaskSchedulerPtr scheduler);

    ~MPPTaskManager() = default;

    std::vector<UInt64> getCurrentQueries();

<<<<<<< HEAD
    std::vector<MPPTaskPtr> getCurrentTasksForQuery(UInt64 query_id);
=======
    bool addMonitoredTask(const String & task_unique_id) { return monitor->addMonitoredTask(task_unique_id); }
>>>>>>> 96a006956b (Fix potential hang when duplicated task registered. (#8193))

    MPPQueryTaskSetPtr getQueryTaskSetWithoutLock(UInt64 query_id);

    bool registerTask(MPPTaskPtr task);

    void unregisterTask(MPPTask * task);

    bool tryToScheduleTask(const MPPTaskPtr & task);

    void releaseThreadsFromScheduler(const int needed_threads);

    MPPTaskPtr findTaskWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout, std::string & errMsg);

    void cancelMPPQuery(UInt64 query_id, const String & reason);

    String toString();
<<<<<<< HEAD
=======

    MPPQueryPtr getMPPQueryWithoutLock(const MPPQueryId & query_id);

    MPPQueryPtr getMPPQuery(const MPPQueryId & query_id);

    /// for test
    MPPQueryId getCurrentMinTSOQueryId(const String & resource_group_name);

    bool isTaskExists(const MPPTaskId & id);

private:
    MPPQueryPtr addMPPQuery(
        const MPPQueryId & query_id,
        bool has_meaningful_gather_id,
        UInt64 auto_spill_check_min_interval_ms);
    void removeMPPGatherTaskSet(MPPQueryPtr & mpp_query, const MPPGatherId & gather_id, bool on_abort);
    std::tuple<MPPQueryPtr, MPPGatherTaskSetPtr, String> getMPPQueryAndGatherTaskSet(const MPPGatherId & gather_id);
>>>>>>> 96a006956b (Fix potential hang when duplicated task registered. (#8193))
};

} // namespace DB
