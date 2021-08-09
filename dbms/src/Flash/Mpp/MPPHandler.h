#pragma once

#include <DataStreams/BlockIO.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/TaskStatus.h>
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

// Identify a mpp task.
struct MPPTaskId
{
    uint64_t start_ts;
    int64_t task_id;
    bool operator<(const MPPTaskId & rhs) const { return start_ts < rhs.start_ts || (start_ts == rhs.start_ts && task_id < rhs.task_id); }
    String toString() const { return "[" + std::to_string(start_ts) + "," + std::to_string(task_id) + "]"; }
};


class MPPTaskManager;

struct MPPTaskProgress
{
    std::atomic<UInt64> current_progress{0};
    UInt64 progress_on_last_check = 0;
    UInt64 epoch_when_found_no_progress = 0;
    bool found_no_progress = false;
    bool isTaskHanging(const Context & context);
};

struct MPPTask : std::enable_shared_from_this<MPPTask>, private boost::noncopyable
{
    Context context;

    std::unique_ptr<tipb::DAGRequest> dag_req;
    std::unique_ptr<DAGContext> dag_context;

    /// store io in MPPTask to keep the life cycle of memory_tracker for the current query
    /// BlockIO contains some information stored in Context and DAGContext, so need deconstruct it before Context and DAGContext
    BlockIO io;
    MemoryTracker * memory_tracker = nullptr;

    MPPTaskId id;

    MPPTaskProgress task_progress;
    std::atomic<Int32> status{INITIALIZING};

    mpp::TaskMeta meta;

    // which targeted task we should send data by which tunnel.
    std::map<MPPTaskId, MPPTunnelPtr> tunnel_map;

    MPPTaskManager * manager = nullptr;

    Logger * log;

    Exception err;

    std::condition_variable cv;

    MPPTask(const mpp::TaskMeta & meta_, const Context & context_)
        : context(context_), meta(meta_), log(&Logger::get("task " + std::to_string(meta_.task_id())))
    {
        id.start_ts = meta.start_ts();
        id.task_id = meta.task_id();
    }

    void unregisterTask();

    void runImpl();

    bool isTaskHanging();

    void cancel(const String & reason);

    /// Similar to `writeErrToAllTunnel`, but it just try to write the error message to tunnel
    /// without waiting the tunnel to be connected
    void closeAllTunnel(const String & reason)
    {
        for (auto & it : tunnel_map)
        {
            it.second->close(reason);
        }
    }

    void finishWrite()
    {
        for (auto it : tunnel_map)
        {
            it.second->writeDone();
        }
    }

    void writeErrToAllTunnel(const String & e);

    std::vector<RegionInfo> prepare(const mpp::DispatchTaskRequest & task_request);

    void updateProgress(const Progress &) { task_progress.current_progress++; }

    void run()
    {
        std::thread worker(&MPPTask::runImpl, this->shared_from_this());
        worker.detach();
    }

    std::mutex tunnel_mutex;

    void registerTunnel(const MPPTaskId & id, MPPTunnelPtr tunnel)
    {
        if (status == CANCELLED)
            throw Exception("the tunnel " + tunnel->id() + " can not been registered, because the task is cancelled");
        std::unique_lock<std::mutex> lk(tunnel_mutex);
        if (tunnel_map.find(id) != tunnel_map.end())
        {
            throw Exception("the tunnel " + tunnel->id() + " has been registered");
        }
        tunnel_map[id] = tunnel;
        cv.notify_all();
    }

    MPPTunnelPtr getTunnelWithTimeout(const ::mpp::EstablishMPPConnectionRequest * request, std::chrono::seconds timeout, String & err_msg)
    {
        MPPTaskId id{request->receiver_meta().start_ts(), request->receiver_meta().task_id()};
        std::map<MPPTaskId, MPPTunnelPtr>::iterator it;
        bool cancelled = false;
        std::unique_lock<std::mutex> lk(tunnel_mutex);
        auto ret = cv.wait_for(lk, timeout, [&] {
            it = tunnel_map.find(id);
            if (status == CANCELLED)
            {
                cancelled = true;
                return true;
            }
            return it != tunnel_map.end();
        });
        if (cancelled)
            err_msg = "can't find tunnel ( " + toString(request->sender_meta().task_id()) + " + "
                + toString(request->receiver_meta().task_id()) + " because the task is cancelled";
        if (!ret)
            err_msg = "can't find tunnel ( " + toString(request->sender_meta().task_id()) + " + "
                + toString(request->receiver_meta().task_id()) + " ) within " + toString(timeout.count()) + " s";
        return (ret && !cancelled) ? it->second : nullptr;
    }
    ~MPPTask()
    {
        /// MPPTask maybe destructed by different thread, set the query memory_tracker
        /// to current_memory_tracker in the destructor
        current_memory_tracker = memory_tracker;
        closeAllTunnel("");
        LOG_DEBUG(log, "finish MPPTask: " << id.toString());
    }
};

using MPPTaskPtr = std::shared_ptr<MPPTask>;

using MPPTaskMap = std::map<MPPTaskId, MPPTaskPtr>;

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
