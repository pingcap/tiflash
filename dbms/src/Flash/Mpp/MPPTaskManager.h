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
#include <mutex>

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

<<<<<<< HEAD
=======
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
    AbortedMPPGatherCache(size_t capacity_)
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

>>>>>>> 306d6b785e (Fix unstable tests and add more ut (#7613))
using MPPQueryTaskSetPtr = std::shared_ptr<MPPQueryTaskSet>;

/// a map from the mpp query id to mpp query task set, we use
/// the query_ts + local_query_id + serverID as the query id, because TiDB can't guarantee
/// the uniqueness of the start ts when stale read or set snapshot
using MPPQueryMap = std::unordered_map<MPPQueryId, MPPQueryTaskSetPtr, MPPQueryIdHash>;

// MPPTaskManger holds all running mpp tasks. It's a single instance holden in Context.
class MPPTaskManager : private boost::noncopyable
{
    MPPTaskSchedulerPtr scheduler;

    std::mutex mu;

    MPPQueryMap mpp_query_map;

    LoggerPtr log;

    std::condition_variable cv;

public:
    explicit MPPTaskManager(MPPTaskSchedulerPtr scheduler);

    ~MPPTaskManager() = default;

    MPPQueryTaskSetPtr getQueryTaskSetWithoutLock(const MPPQueryId & query_id);

<<<<<<< HEAD
    MPPQueryTaskSetPtr getQueryTaskSet(const MPPQueryId & query_id);
=======
    void addMonitoredTask(const String & task_unique_id) { monitor->addMonitoredTask(task_unique_id); }

    void removeMonitoredTask(const String & task_unique_id) { monitor->removeMonitoredTask(task_unique_id); }

    std::pair<MPPQueryTaskSetPtr, String> getQueryTaskSetWithoutLock(const MPPQueryId & query_id);

    std::pair<MPPQueryTaskSetPtr, String> getQueryTaskSet(const MPPQueryId & query_id);
>>>>>>> 306d6b785e (Fix unstable tests and add more ut (#7613))

    std::pair<bool, String> registerTask(MPPTaskPtr task);

    std::pair<bool, String> unregisterTask(const MPPTaskId & id);

    bool tryToScheduleTask(MPPTaskScheduleEntry & schedule_entry);

    void releaseThreadsFromScheduler(int needed_threads);

    std::pair<MPPTunnelPtr, String> findTunnelWithTimeout(const ::mpp::EstablishMPPConnectionRequest * request, std::chrono::seconds timeout);

    std::pair<MPPTunnelPtr, String> findAsyncTunnel(const ::mpp::EstablishMPPConnectionRequest * request, EstablishCallData * call_data, grpc::CompletionQueue * cq);

    void abortMPPQuery(const MPPQueryId & query_id, const String & reason, AbortType abort_type);

    String toString();

private:
    MPPQueryTaskSetPtr addMPPQueryTaskSet(const MPPQueryId & query_id);
    void removeMPPQueryTaskSet(const MPPQueryId & query_id, bool on_abort);
};

} // namespace DB
