#pragma once

#include <DataStreams/BlockIO.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGDriver.h>
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


struct MPPTask;
struct MPPTunnel
{
    std::mutex mu;
    std::condition_variable cv_for_connected;
    std::condition_variable cv_for_finished;

    bool connected; // if the exchange in has connected this tunnel.

    bool finished; // if the tunnel has finished its connection.

    ::grpc::ServerWriter<::mpp::MPPDataPacket> * writer;

    std::chrono::seconds timeout;

    std::weak_ptr<MPPTask> current_task;

    // tunnel id is in the format like "tunnel[sender]+[receiver]"
    String tunnel_id;

    Logger * log;

    MPPTunnel(const mpp::TaskMeta & receiver_meta_, const mpp::TaskMeta & sender_meta_, const std::chrono::seconds timeout_,
        std::shared_ptr<MPPTask> current_task_)
        : connected(false),
          finished(false),
          timeout(timeout_),
          current_task(current_task_),
          tunnel_id("tunnel" + std::to_string(sender_meta_.task_id()) + "+" + std::to_string(receiver_meta_.task_id())),
          log(&Logger::get(tunnel_id))
    {}

    ~MPPTunnel()
    {
        try
        {
            if (!finished)
                writeDone();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Error in destructor function of MPPTunnel");
        }
    }

    bool isTaskCancelled();

    void waitUntilConnect(std::unique_lock<std::mutex> & lk);
    // write a single packet to the tunnel, it will block if tunnel is not ready.
    // TODO: consider to hold a buffer
    void write(const mpp::MPPDataPacket & data, bool close_after_write = false)
    {

        LOG_TRACE(log, "ready to write");
        std::unique_lock<std::mutex> lk(mu);

        waitUntilConnect(lk);
        if (finished)
            throw Exception("write to tunnel which is already closed.");
        if (!writer->Write(data))
            throw Exception("Failed to write data");
        if (close_after_write)
        {
            finished = true;
            cv_for_finished.notify_all();
        }
        if (close_after_write)
            LOG_TRACE(log, "finish write and close the tunnel");
        else
            LOG_TRACE(log, "finish write");
    }

    // finish the writing.
    void writeDone()
    {
        std::unique_lock<std::mutex> lk(mu);
        if (finished)
            throw Exception("has finished");
        /// make sure to finish the tunnel after it is connected
        waitUntilConnect(lk);
        finished = true;
        cv_for_finished.notify_all();
    }

    /// close() finishes the tunnel, if the tunnel is connected already, it will
    /// write the error message to the tunnel, otherwise it just close the tunnel
    void close(const String & reason);

    // a MPPConn request has arrived. it will build connection by this tunnel;
    void connect(::grpc::ServerWriter<::mpp::MPPDataPacket> * writer_)
    {
        std::lock_guard<std::mutex> lk(mu);
        if (connected)
        {
            throw Exception("has connected");
        }
        if (finished)
            throw Exception("has finished");
        LOG_DEBUG(log, "ready to connect");
        connected = true;
        writer = writer_;

        cv_for_connected.notify_all();
    }

    // wait until all the data has been transferred.
    void waitForFinish()
    {
        std::unique_lock<std::mutex> lk(mu);

        cv_for_finished.wait(lk, [&]() { return finished; });
    }
};

using MPPTunnelPtr = std::shared_ptr<MPPTunnel>;

struct MPPTunnelSet
{
    std::vector<MPPTunnelPtr> tunnels;

    void clearExecutionSummaries(tipb::SelectResponse & response)
    {
        /// can not use response.clear_execution_summaries() because
        /// TiDB assume all the executor should return execution summary
        for (int i = 0; i < response.execution_summaries_size(); i++)
        {
            auto * mutable_execution_summary = response.mutable_execution_summaries(i);
            mutable_execution_summary->set_num_produced_rows(0);
            mutable_execution_summary->set_num_iterations(0);
            mutable_execution_summary->set_concurrency(0);
        }
    }
    /// for both broadcast writing and partition writing, only
    /// return meaningful execution summary for the first tunnel,
    /// because in TiDB, it does not know enough information
    /// about the execution details for the mpp query, it just
    /// add up all the execution summaries for the same executor,
    /// so if return execution summary for all the tunnels, the
    /// information in TiDB will be amplified, which may make
    /// user confused.
    // this is a broadcast writing.
    void write(tipb::SelectResponse & response)
    {
        std::string data;
        if (!response.SerializeToString(&data))
            throw Exception("Fail to serialize response, response size: " + std::to_string(response.ByteSizeLong()));
        mpp::MPPDataPacket packet;
        packet.set_data(data);
        tunnels[0]->write(packet);

        if (tunnels.size() > 1)
        {
            clearExecutionSummaries(response);
            data.clear();
            if (!response.SerializeToString(&data))
                throw Exception("Fail to serialize response, response size: " + std::to_string(response.ByteSizeLong()));
            packet.set_data(data);
            for (size_t i = 1; i < tunnels.size(); i++)
            {
                tunnels[i]->write(packet);
            }
        }
    }

    // this is a partition writing.
    void write(tipb::SelectResponse & response, int16_t partition_id)
    {
        if (partition_id != 0)
        {
            clearExecutionSummaries(response);
        }
        std::string data;
        if (!response.SerializeToString(&data))
            throw Exception("Fail to serialize response, response size: " + std::to_string(response.ByteSizeLong()));
        mpp::MPPDataPacket packet;
        packet.set_data(data);
        tunnels[partition_id]->write(packet);
    }

    uint16_t getPartitionNum() { return tunnels.size(); }
};

using MPPTunnelSetPtr = std::shared_ptr<MPPTunnelSet>;

class MPPTaskManager;

struct MPPTaskProgress
{
    std::atomic<UInt64> current_progress{0};
    UInt64 progress_on_last_check = 0;
    UInt64 epoch_when_found_no_progress = 0;
    bool found_no_progress = false;
    bool isTaskHanging(const Context & context);
};

enum TaskStatus
{
    INITIALIZING,
    RUNNING,
    FINISHED,
    CANCELLED,
};

struct MPPTask : std::enable_shared_from_this<MPPTask>, private boost::noncopyable
{

    std::unique_ptr<tipb::DAGRequest> dag_req;

    Context context;
    /// store io in MPPTask to keep the life cycle of memory_tracker for the current query
    /// BlockIO contains some information stored in Context, so need deconstruct it before Context
    BlockIO io;
    /// The inputStreams should be released in the destructor of BlockIO, since DAGContext contains
    /// some reference to inputStreams, so it need to be destructed before BlockIO
    std::unique_ptr<DAGContext> dag_context;
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
            throw Exception("the tunnel " + tunnel->tunnel_id + " can not been registered, because the task is cancelled");
        std::unique_lock<std::mutex> lk(tunnel_mutex);
        if (tunnel_map.find(id) != tunnel_map.end())
        {
            throw Exception("the tunnel " + tunnel->tunnel_id + " has been registered");
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
        if (current_memory_tracker != memory_tracker)
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
