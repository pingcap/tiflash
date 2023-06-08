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

using MPPQueryTaskSetPtr = std::shared_ptr<MPPQueryTaskSet>;

>>>>>>> 306d6b785e (Fix unstable tests and add more ut (#7613))
/// a map from the mpp query id to mpp query task set, we use
/// the start ts of a query as the query id as TiDB will guarantee
/// the uniqueness of the start ts
using MPPQueryMap = std::unordered_map<UInt64, MPPQueryTaskSet>;

// MPPTaskManger holds all running mpp tasks. It's a single instance holden in Context.
class MPPTaskManager : private boost::noncopyable
{
    std::mutex mu;

    MPPQueryMap mpp_query_map;

    Poco::Logger * log;

    std::condition_variable cv;

public:
    MPPTaskManager();
    ~MPPTaskManager();

    std::vector<UInt64> getCurrentQueries();

    std::vector<MPPTaskPtr> getCurrentTasksForQuery(UInt64 query_id);

    bool registerTask(MPPTaskPtr task);

<<<<<<< HEAD
    void unregisterTask(MPPTask * task);

    MPPTaskPtr findTaskWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout, std::string & errMsg);
=======
    std::pair<MPPQueryTaskSetPtr, String> getQueryTaskSetWithoutLock(const MPPQueryId & query_id);

    std::pair<MPPQueryTaskSetPtr, String> getQueryTaskSet(const MPPQueryId & query_id);
>>>>>>> 306d6b785e (Fix unstable tests and add more ut (#7613))

    void cancelMPPQuery(UInt64 query_id, const String & reason);

    String toString();
};

} // namespace DB
