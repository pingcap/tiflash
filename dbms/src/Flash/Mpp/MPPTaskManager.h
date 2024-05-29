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

#include <Core/QueryOperatorSpillContexts.h>
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
struct MPPGatherTaskSet
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
    /// <sender_task_id, <receiver_task_id, alarm>>
    std::unordered_map<Int64, std::unordered_map<Int64, grpc::Alarm>> alarms;
    /// only used in scheduler
    std::queue<MPPTaskId> waiting_tasks;
    bool isInNormalState() const { return state == Normal; }
    bool allowUnregisterTask() const { return state == Normal || state == Aborted; }
    MPPTask * findMPPTask(const MPPTaskId & task_id) const;
    bool isTaskRegistered(const MPPTaskId & task_id) const { return task_map.find(task_id) != task_map.end(); }
    std::pair<bool, String> isTaskAlreadyFinishedOrFailed(const MPPTaskId & task_id) const
    {
        auto result = finished_or_failed_tasks.find(task_id);
        if (result != finished_or_failed_tasks.end())
            return {true, result->second};
        else
            return {false, ""};
    }
    void registerTask(const MPPTaskId & task_id)
    {
        assert(task_map.find(task_id) == task_map.end());
        task_map[task_id] = nullptr;
    }
    void makeTaskActive(const MPPTaskPtr & task)
    {
        assert(task_map.find(task->getId()) != task_map.end());
        task_map[task->getId()] = task;
    }
    void cancelAlarmsBySenderTaskId(const MPPTaskId & task_id);
    bool hasMPPTask() const { return !task_map.empty(); }
    bool hasAlarm() const { return !alarms.empty(); }
    template <typename F>
    void forEachMPPTask(F && f) const
    {
        for (const auto & it : task_map)
            f(it);
    }
    void markTaskAsFinishedOrFailed(const MPPTaskId & task_id, const String & error_message);

private:
    MPPTaskMap task_map;
    std::unordered_map<MPPTaskId, String> finished_or_failed_tasks;
};
using MPPGatherTaskSetPtr = std::shared_ptr<MPPGatherTaskSet>;

struct MPPQuery
{
    MPPQuery(const MPPQueryId & mpp_query_id, bool has_meaningful_gather_id_, UInt64 auto_spill_check_min_interval_ms)
        : mpp_query_operator_spill_contexts(
            std::make_shared<QueryOperatorSpillContexts>(mpp_query_id, auto_spill_check_min_interval_ms))
        , has_meaningful_gather_id(has_meaningful_gather_id_)
    {}
    MPPGatherTaskSetPtr addMPPGatherTaskSet(const MPPGatherId & gather_id);
    ~MPPQuery();

    std::shared_ptr<ProcessListEntry> process_list_entry;
    std::unordered_map<MPPGatherId, MPPGatherTaskSetPtr, MPPGatherIdHash> mpp_gathers;
    std::shared_ptr<QueryOperatorSpillContexts> mpp_query_operator_spill_contexts;
    bool has_meaningful_gather_id;
};
using MPPQueryPtr = std::shared_ptr<MPPQuery>;


/// A simple thread unsafe FIFO cache used to fix the "lost cancel" issues
static const size_t ABORTED_MPPGATHER_CACHE_SIZE = 1000;
static const size_t MAX_ABORTED_REASON_LENGTH = 500;
/// the cache size is about (2 * sizeof(MPPGatherId) + MAX_ABORTED_REASON_LENGTH) * ABORTED_MPPGATHER_CACHE_SIZE, it should be less than 1MB
class AbortedMPPGatherCache
{
private:
    std::deque<MPPGatherId> gather_ids;
    std::unordered_map<MPPGatherId, String, MPPGatherIdHash> gather_ids_set;
    size_t capacity;

public:
    explicit AbortedMPPGatherCache(size_t capacity_)
        : capacity(capacity_)
    {}
    /// return aborted_reason if the mpp gather is aborted, otherwise, return empty string
    String check(const MPPGatherId & id)
    {
        assert(gather_ids_set.size() == gather_ids.size());
        if (gather_ids_set.find(id) != gather_ids_set.end())
            return gather_ids_set[id];
        else
            return "";
    }
    void add(const MPPGatherId & id, const String abort_reason)
    {
        assert(gather_ids_set.size() == gather_ids.size());
        if (gather_ids_set.find(id) != gather_ids_set.end())
            return;
        if (gather_ids_set.size() >= capacity)
        {
            auto evicted_id = gather_ids.back();
            gather_ids.pop_back();
            gather_ids_set.erase(evicted_id);
        }
        gather_ids.push_front(id);
        if unlikely (abort_reason.empty())
            gather_ids_set[id] = "query is aborted";
        else
            gather_ids_set[id] = abort_reason.substr(0, MAX_ABORTED_REASON_LENGTH);
    }
};

/// a map from the mpp query id to mpp query, we use
/// the query_ts + local_query_id + serverID as the query id, because TiDB can't guarantee
/// the uniqueness of the start ts when stale read or set snapshot
using MPPQueryMap = std::unordered_map<MPPQueryId, MPPQueryPtr, MPPQueryIdHash>;

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

// MPPTaskManger holds all running mpp tasks. It's a single instance holden in Context.
class MPPTaskManager : private boost::noncopyable
{
    MPPTaskSchedulerPtr scheduler;

    std::mutex mu;

    MPPQueryMap mpp_query_map;

    AbortedMPPGatherCache aborted_query_gather_cache;

    LoggerPtr log;

    std::condition_variable cv;

    std::shared_ptr<MPPTaskMonitor> monitor;

public:
    explicit MPPTaskManager(MPPTaskSchedulerPtr scheduler);

    ~MPPTaskManager();

    void shutdown();

    std::shared_ptr<MPPTaskMonitor> getMPPTaskMonitor() const { return monitor; }

    bool addMonitoredTask(const String & task_unique_id) { return monitor->addMonitoredTask(task_unique_id); }

    void removeMonitoredTask(const String & task_unique_id) { monitor->removeMonitoredTask(task_unique_id); }

    std::pair<MPPGatherTaskSetPtr, String> getGatherTaskSetWithoutLock(const MPPGatherId & gather_id);

    std::pair<MPPGatherTaskSetPtr, String> getGatherTaskSet(const MPPGatherId & gather_id);

    /// registerTask make the task info stored in MPPTaskManager, but it is still not visible to other mpp tasks before makeTaskActive.
    /// After registerTask, the related query_task_set can't be cleaned before unregisterTask is called
    std::pair<bool, String> registerTask(MPPTask * task);

    std::pair<bool, String> makeTaskActive(MPPTaskPtr task);

    std::pair<bool, String> unregisterTask(const MPPTaskId & id, const String & error_message);

    bool tryToScheduleTask(MPPTaskScheduleEntry & schedule_entry);

    void releaseThreadsFromScheduler(const String & resource_group_name, int needed_threads);

    std::pair<MPPTunnelPtr, String> findTunnelWithTimeout(
        const ::mpp::EstablishMPPConnectionRequest * request,
        std::chrono::seconds timeout);

    std::pair<MPPTunnelPtr, String> findAsyncTunnel(
        const ::mpp::EstablishMPPConnectionRequest * request,
        EstablishCallData * call_data,
        grpc::CompletionQueue * cq,
        const Context & context);

    void abortMPPGather(const MPPGatherId & gather_id, const String & reason, AbortType abort_type);

    String toString();

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
};

} // namespace DB
