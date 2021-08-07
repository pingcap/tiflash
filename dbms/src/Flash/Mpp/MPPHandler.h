#pragma once

#include <DataStreams/BlockIO.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/Utils.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>
#include <condition_variable>
#include <mutex>
#include <thread>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Common/MemoryTracker.h>
#include <kvproto/mpp.pb.h>
#include <kvproto/tikvpb.grpc.pb.h>
#pragma GCC diagnostic pop

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
    std::mutex mu;

    MPPQueryMap mpp_query_map;

    Logger * log;

    std::condition_variable cv;

public:
    MPPTaskManager();
    ~MPPTaskManager();

    std::vector<UInt64> getCurrentQueries()
    {
        std::vector<UInt64> ret;
        std::lock_guard<std::mutex> lock(mu);
        for (auto & it : mpp_query_map)
        {
            ret.push_back(it.first);
        }
        return ret;
    }

    std::vector<MPPTaskPtr> getCurrentTasksForQuery(UInt64 query_id)
    {
        std::vector<MPPTaskPtr> ret;
        std::lock_guard<std::mutex> lock(mu);
        const auto & it = mpp_query_map.find(query_id);
        if (it == mpp_query_map.end() || it->second.to_be_cancelled)
            return ret;
        for (const auto & task_it : it->second.task_map)
            ret.push_back(task_it.second);
        return ret;
    }

    bool registerTask(MPPTaskPtr task);

    void unregisterTask(MPPTask * task);

    MPPTaskPtr findTaskWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout, std::string & errMsg);

    void cancelMPPQuery(UInt64 query_id, const String & reason);

    String toString()
    {
        std::lock_guard<std::mutex> lock(mu);
        String res("(");
        for (auto & query_it : mpp_query_map)
        {
            for (auto & it : query_it.second.task_map)
                res += it.first.toString() + ", ";
        }
        return res + ")";
    }
};

class MPPHandler
{
    const mpp::DispatchTaskRequest & task_request;

    Logger * log;

public:
    MPPHandler(const mpp::DispatchTaskRequest & task_request_) : task_request(task_request_), log(&Logger::get("MPPHandler")) {}
    grpc::Status execute(Context & context, mpp::DispatchTaskResponse * response);
    void handleError(MPPTaskPtr task, String error);
};

} // namespace DB
