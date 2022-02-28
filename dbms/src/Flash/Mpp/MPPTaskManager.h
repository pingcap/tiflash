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
    std::atomic<bool> to_be_cancelled = false;
    MPPTaskMap task_map;
    /// only used in scheduler
    UInt32 scheduled_task = 0;
    UInt32 used_threads = 0;
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

    Poco::Logger * log;

    std::condition_variable cv;

public:
    MPPTaskManager(MPPTaskSchedulerPtr);
    ~MPPTaskManager() = default;

    std::vector<UInt64> getCurrentQueries();

    std::vector<MPPTaskPtr> getCurrentTasksForQuery(UInt64 query_id);

    MPPQueryTaskSetPtr getQueryTaskSetWithoutLock(UInt64 query_id);

    bool registerTask(MPPTaskPtr task);

    void unregisterTask(MPPTask * task);

    bool tryToScheduleTask(MPPTaskPtr task);

    MPPTaskPtr findTaskWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout, std::string & errMsg);

    void cancelMPPQuery(UInt64 query_id, const String & reason);

    String toString();
};

} // namespace DB
