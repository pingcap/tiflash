#pragma once

#include <Flash/Mpp/MPPTask.h>
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
};

/// a map from the mpp query id to mpp query task set, we use
/// the start ts of a query as the query id as TiDB will guarantee
/// the uniqueness of the start ts
using MPPQueryMap = std::unordered_map<UInt64, MPPQueryTaskSet>;

// MPPTaskManger holds all running mpp tasks. It's a single instance holden in Context.
class MPPTaskManager : private boost::noncopyable
{
    std::mutex pri_mu;
    const int bucket_num = 10000;
    std::mutex * mu_arr = new std::mutex[bucket_num];
    MPPQueryMap * mpp_query_maps = new MPPQueryMap[bucket_num];
    std::unordered_map<UInt64, std::shared_ptr<std::unordered_map<MPPTaskId, std::vector<EstablishCallData *>>>> * wait_maps = new std::unordered_map<UInt64, std::shared_ptr<std::unordered_map<MPPTaskId, std::vector<EstablishCallData *>>>>[bucket_num];
    std::priority_queue<std::pair<long, MPPTaskId>, std::vector<std::pair<long, MPPTaskId>>, auto (*)(const std::pair<long, MPPTaskId> &, const std::pair<long, MPPTaskId> &)->bool> wait_deadline_queue{
        [](const std::pair<long, MPPTaskId> & a, const std::pair<long, MPPTaskId> & b) -> bool {
            return a.first > b.first;
        }};


    Poco::Logger * log;

    std::condition_variable cv;
    std::atomic<bool> end_syn{false}, end_fin{false};
    std::shared_ptr<std::thread> bk_thd;

public:
    MPPTaskManager();
    ~MPPTaskManager();

    void BackgroundJob();
    void ClearTimeoutWaiter(const MPPTaskId & id);

    template <class Mp, class Itr>
    void notifyWaiters(Mp & wait_map,
                       const Itr & wait_it,
                       const MPPTaskId & id,
                       std::function<void(EstablishCallData *)> job);

    template <class Mp, class Itr>
    void notifyQueryWaiters(Mp & wait_map,
                            const Itr & wait_it,
                            std::function<void(const MPPTaskId & id, EstablishCallData *)> job);

    int computeBucketId(UInt64 start_ts)
    {
        return start_ts % bucket_num;
    }

    std::vector<UInt64> getCurrentQueries();

    std::vector<MPPTaskPtr> getCurrentTasksForQuery(UInt64 query_id);

    bool registerTask(MPPTaskPtr task);

    void unregisterTask(MPPTask * task);

    MPPTaskPtr findTaskWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout, std::string & errMsg, EstablishCallData * async_calldata = nullptr);

    void cancelMPPQuery(UInt64 query_id, const String & reason);

    String toString();
};

} // namespace DB
