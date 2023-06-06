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

/// A simple thread unsafe FIFO cache used to fix the "lost cancel" issues
class AbortedMPPGatherCache
{
private:
    std::deque<MPPGatherId> gather_ids;
    std::unordered_set<MPPGatherId, MPPGatherIdHash> gather_ids_set;
    size_t capacity;

public:
    AbortedMPPGatherCache(size_t capacity_)
        : capacity(capacity_)
    {}
    bool exists(const MPPGatherId & id)
    {
        assert(gather_ids_set.size() == gather_ids.size());
        return gather_ids_set.find(id) != gather_ids_set.end();
    }
    void add(const MPPGatherId & id)
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
        gather_ids_set.insert(id);
    }
};

using MPPQueryTaskSetPtr = std::shared_ptr<MPPQueryTaskSet>;

/// a map from the mpp query id to mpp query task set, we use
/// the start ts of a query as the query id as TiDB will guarantee
/// the uniqueness of the start ts
using MPPQueryMap = std::unordered_map<UInt64, MPPQueryTaskSetPtr>;

// MPPTaskManger holds all running mpp tasks. It's a single instance holden in Context.
class MPPTaskManager : private boost::noncopyable
{
    MPPTaskSchedulerPtr scheduler;

    std::mutex mu;

    MPPQueryMap mpp_query_map;

    AbortedMPPGatherCache aborted_query_gather_cache;

    LoggerPtr log;

    std::condition_variable cv;

public:
    explicit MPPTaskManager(MPPTaskSchedulerPtr scheduler);

    ~MPPTaskManager() = default;

    MPPQueryTaskSetPtr getQueryTaskSetWithoutLock(UInt64 query_id);

<<<<<<< HEAD
    MPPQueryTaskSetPtr getQueryTaskSet(UInt64 query_id);
=======
    void addMonitoredTask(const String & task_unique_id) { monitor->addMonitoredTask(task_unique_id); }

    void removeMonitoredTask(const String & task_unique_id) { monitor->removeMonitoredTask(task_unique_id); }

    std::pair<MPPQueryTaskSetPtr, bool> getQueryTaskSetWithoutLock(const MPPQueryId & query_id);

    std::pair<MPPQueryTaskSetPtr, bool> getQueryTaskSet(const MPPQueryId & query_id);
>>>>>>> 12435a7c05 (Fix "lost cancel" for mpp query (#7589))

    std::pair<bool, String> registerTask(MPPTaskPtr task);

    std::pair<bool, String> unregisterTask(const MPPTaskId & id);

    bool tryToScheduleTask(MPPTaskScheduleEntry & schedule_entry);

    void releaseThreadsFromScheduler(int needed_threads);

    std::pair<MPPTunnelPtr, String> findTunnelWithTimeout(const ::mpp::EstablishMPPConnectionRequest * request, std::chrono::seconds timeout);

    std::pair<MPPTunnelPtr, String> findAsyncTunnel(const ::mpp::EstablishMPPConnectionRequest * request, EstablishCallData * call_data, grpc::CompletionQueue * cq);

    void abortMPPQuery(UInt64 query_id, const String & reason, AbortType abort_type);

    String toString();

private:
    MPPQueryTaskSetPtr addMPPQueryTaskSet(UInt64 query_id);
    void removeMPPQueryTaskSet(UInt64 query_id, bool on_abort);
};

} // namespace DB
