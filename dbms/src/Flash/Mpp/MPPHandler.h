#pragma once

#include <DataStreams/BlockIO.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/copyData.h>
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

    MPPTaskId(const mpp::TaskMeta & meta) : start_ts(meta.start_ts()), task_id(meta.task_id()) {}

    bool operator<(const MPPTaskId & rhs) const { return start_ts < rhs.start_ts || (start_ts == rhs.start_ts && task_id < rhs.task_id); }
    String toString() const { return "[" + std::to_string(start_ts) + "," + std::to_string(task_id) + "]"; }
};

struct MPPTunnel
{
    std::mutex mu;
    //    std::condition_variable cv_for_connected;
    //    std::condition_variable cv_for_finished;

    bool connected; // if the exchange in has connected this tunnel.

    //    bool finished; // if the tunnel has finished its connection.

    ::grpc::ServerWriter<::mpp::MPPDataPacket> * writer;

    std::chrono::seconds timeout;

    // tunnel id is in the format like "tunnel[sender]+[receiver]"
    String tunnel_id;

    Logger * log;

    MPPTunnel(const mpp::TaskMeta & receiver_meta_, const mpp::TaskMeta & sender_meta_, const std::chrono::seconds timeout_)
        : connected(false),
          //          finished(false),
          timeout(timeout_),
          tunnel_id("tunnel" + std::to_string(sender_meta_.task_id()) + "+" + std::to_string(receiver_meta_.task_id())),
          log(&Logger::get(tunnel_id))
    {}

    //    ~MPPTunnel()
    //    {
    //        try
    //        {
    //            if (!finished)
    //                writeDone();
    //        }
    //        catch (...)
    //        {
    //            tryLogCurrentException(log, "Error in destructor function of MPPTunnel");
    //        }
    //    }

    // write a single packet to the tunnel, it will block if tunnel is not ready.
    // TODO: consider to hold a buffer
    void write(const mpp::MPPDataPacket & data)
    {
        LOG_TRACE(log, "ready to write");
        std::unique_lock<std::mutex> lk(mu);

        if (!connected)
            throw Exception("write to tunnel which is not yet connected.");
        writer->Write(data);
    }

    //    // finish the writing.
    //    void writeDone()
    //    {
    //        std::unique_lock<std::mutex> lk(mu);
    //        if (finished)
    //            throw Exception("has finished");
    //        /// make sure to finish the tunnel after it is connected
    //        if (timeout.count() > 0)
    //        {
    //            if (!cv_for_connected.wait_for(lk, timeout, [&]() { return connected; }))
    //            {
    //                throw Exception(tunnel_id + " is timeout");
    //            }
    //        }
    //        else
    //        {
    //            cv_for_connected.wait(lk, [&]() { return connected; });
    //        }
    //        finished = true;
    //        cv_for_finished.notify_all();
    //    }

    /// close() finishes the tunnel, if the tunnel is connected already, it will
    /// write the error message to the tunnel, otherwise it just close the tunnel
    //    void close(const String & reason);

    // a MPPConn request has arrived. it will build connection by this tunnel;
    void connect(::grpc::ServerWriter<::mpp::MPPDataPacket> * writer_)
    {
        std::lock_guard<std::mutex> lk(mu);

        if (connected)
            throw Exception("has connected");

        LOG_DEBUG(log, "ready to connect");
        connected = true;
        writer = writer_;
    }

    // wait until all the data has been transferred.
    //    void waitForFinish()
    //    {
    //        std::unique_lock<std::mutex> lk(mu);
    //
    //        cv_for_finished.wait(lk, [&]() { return finished; });
    //    }
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
        response.SerializeToString(&data);
        mpp::MPPDataPacket packet;
        packet.set_data(data);
        tunnels[0]->write(packet);

        if (tunnels.size() > 1)
        {
            clearExecutionSummaries(response);
            data.clear();
            response.SerializeToString(&data);
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
        response.SerializeToString(&data);
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


class MPPTaskProxy;
class TunnelWriterStatus;

using MPPTaskProxyPtr = std::shared_ptr<MPPTaskProxy>;
using MPPTaskProxyWeakPtr = std::weak_ptr<MPPTaskProxy>;
using TunnelWriterStatusPtr = std::shared_ptr<TunnelWriterStatus>;
using TunnelWriterStatusMap = std::map<MPPTaskId, TunnelWriterStatusPtr>;

class TunnelWriterStatus
{
    using Writer = grpc::ServerWriter<::mpp::MPPDataPacket>;

private:
    Writer * writer;

    bool connected = false; // Connected to the corresponding tunnel or not.
    bool finished = false;  // Finised or not, could be done writing, or cancelled.

    std::mutex mutex;
    std::condition_variable cv;

public:
    TunnelWriterStatus(Writer * writer_) : writer(writer_) {}

    void waitForFinished()
    {
        std::unique_lock<std::mutex> lk(mutex);
        cv.wait(lk, [&]() { return finished; });
    }

    void setFinished()
    {
        std::unique_lock<std::mutex> lk(mutex);
        finished = true;
        cv.notify_all();
    }

    auto getWriter()
    {
        std::unique_lock<std::mutex> lk(mutex);

        return std::make_pair(connected, writer);
    }
};

/**
 * The worker thread of MPPTask owns strong references to a MPPTask and a MPPTaskProxy.
 * Other threads only have weak references to MPPTaskProxy.
 *
 * And all other threads besides the worker thread talk to MPPTaskProxy. The worker thread will periodically check
 * the MPPTaskProxy, read the messages in it, and apply to MPPTask.
 */
class MPPTaskProxy
{
private:
    MPPTaskId task_id;
    bool cancelled = false;
    String cancel_reason;
    bool has_new_writer = false;
    TunnelWriterStatusMap writer_status_map;

    std::mutex mutex;
    std::condition_variable cv;

public:
    MPPTaskProxy(const MPPTaskId & task_id_) : task_id(task_id_) {}

    ~MPPTaskProxy() { notifyFinished(); }

    void cancelTask(const String & cancel_reason_)
    {
        std::unique_lock<std::mutex> lock(mutex);

        cancelled = true;
        cancel_reason = cancel_reason_;

        cv.notify_all();
    }

    void notifyFinished()
    {
        std::unique_lock<std::mutex> lock(mutex);

        // Set the finished status, to make the threads of FlashService::EstablishMPPConnection to continue.
        for (auto [_, tunel_writer_status] : writer_status_map)
        {
            (void)_;
            tunel_writer_status->setFinished();
        }
    }

    TunnelWriterStatusPtr setTunnelWriter(
        const mpp::TaskMeta & receiver_meta, grpc::ServerWriter<::mpp::MPPDataPacket> * writer, Logger * logger)
    {
        std::unique_lock<std::mutex> lock(mutex);

        MPPTaskId id(receiver_meta);
        if (writer_status_map.count(id))
        {
            String msg = "Tunnel writer conflict: " + id.toString();
            LOG_ERROR(logger, msg);
            throw Exception(msg);
        }

        auto tunnel_writer_status = std::make_shared<TunnelWriterStatus>(writer);

        writer_status_map.emplace(id, tunnel_writer_status);
        has_new_writer = true;

        cv.notify_all();

        return tunnel_writer_status;
    }

    bool hasCancelled()
    {
        std::unique_lock<std::mutex> lock(mutex);
        return cancelled;
    }

    std::tuple<bool, bool, TunnelWriterStatusMap> getNewTunnelWriters()
    {
        std::unique_lock<std::mutex> lock(mutex);

        // TODO: put into configuration.
        std::chrono::seconds timeout(10);

        if (!cv.wait_for(lock, timeout, [&]() { return has_new_writer || cancelled; }))
        {
            throw Exception(__FUNCTION__ + task_id.toString() + " is timeout");
        }

        if (has_new_writer)
        {
            has_new_writer = false;
            return std::make_tuple(cancelled, true, writer_status_map);
        }
        else
        {
            return std::make_tuple(cancelled, false, TunnelWriterStatusMap{});
        }
    }
};

enum TaskStatus
{
    INITIALIZING,
    RUNNING,
    //    FINISHED,
    CANCELLED,
};

struct MPPTask : std::enable_shared_from_this<MPPTask>, private boost::noncopyable
{
    Context context;

    /// store io in MPPTask to keep the life cycle of memory_tracker for the current query
    BlockIO io;

    std::unique_ptr<tipb::DAGRequest> dag_req;
    std::unique_ptr<DAGContext> dag_context;
    MemoryTracker * memory_tracker = nullptr;

    MPPTaskId task_id;

    MPPTaskProgress task_progress;

    // which targeted task we should send data by which tunnel.
    std::map<MPPTaskId, MPPTunnelPtr> tunnel_map;
    size_t connected_tunel_count = 0;

    MPPTaskManager * manager = nullptr;

    Logger * log;

    Exception err;

    //    std::mutex mutex;
    //    std::condition_variable cv;

    MPPTask(const MPPTaskId & task_id_, const Context & context_)
        : context(context_), task_id(task_id_), log(&Logger::get("task " + task_id.toString()))
    {}

    void runImpl(const MPPTaskProxyPtr & proxy);

    void handleError(String error);

    std::unordered_map<RegionVerID, RegionInfo> prepare(const mpp::DispatchTaskRequest & task_request);

    std::vector<RegionInfo> initStreams(
        const mpp::DispatchTaskRequest & task_request, std::unordered_map<RegionVerID, RegionInfo> & regions);


    ~MPPTask()
    {
        /// MPPTask maybe destructed by different thread, set the query memory_tracker
        /// to current_memory_tracker in the destructor
        current_memory_tracker = memory_tracker;
        //        closeAllTunnel("MPPTask release");
        LOG_DEBUG(log, "finish MPPTask: " << task_id.toString());
    }

private:
    void unregisterTask();

    /// Similar to `writeErrToAllTunnel`, but it just try to write the error message to tunnel
    /// without waiting the tunnel to be connected
    //    void closeAllTunnel(const String & reason)
    //    {
    //        for (auto & it : tunnel_map)
    //        {
    //            it.second->close(reason);
    //        }
    //    }

    void registerTunnel(const MPPTaskId & id_, MPPTunnelPtr tunnel)
    {
        if (tunnel_map.find(id_) != tunnel_map.end())
        {
            throw Exception("the tunnel " + tunnel->tunnel_id + " has been registered");
        }
        tunnel_map[id_] = tunnel;
    }

    bool allTunnelConnected() { return connected_tunel_count == tunnel_map.size(); }

    void connectTunnelWriters(const TunnelWriterStatusMap & tunnel_writer_maps)
    {
        for (auto && [task_id, tunel_writer_status] : tunnel_writer_maps)
        {
            auto && [connected, writer] = tunel_writer_status->getWriter();
            if (connected) // This writer is already connected to its tunnel.
                continue;

            auto itr = tunnel_map.find(task_id);
            if (itr == tunnel_map.end())
                throw Exception("Can not find corresponding tunnel for writer: " + task_id.toString());
            itr->second->connect(writer);
            connected_tunel_count++;
        }
    }

    void writeErrToAllTunnel(const String & e);

    //    void finishWrite()
    //    {
    //        for (auto it : tunnel_map)
    //        {
    //            it.second->writeDone();
    //        }
    //    }
};

using MPPTaskPtr = std::shared_ptr<MPPTask>;


using MPPTaskMap = std::map<MPPTaskId, MPPTaskProxyWeakPtr>;

struct MPPQueryTaskSet
{
    /// to_be_cancelled is kind of lock, if to_be_cancelled is set
    /// to true, then task_map can only be modified by query cancel
    /// thread, which means no task can register/un-register for the
    /// query, here we do not need mutex because all the write/read
    /// to MPPQueryTaskSet is protected by the mutex in MPPTaskManager
    bool to_be_cancelled = false;
    MPPTaskMap task_map;

    void swap(MPPQueryTaskSet & o)
    {
        std::swap(to_be_cancelled, o.to_be_cancelled);
        task_map.swap(o.task_map);
    }
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

    bool registerTask(const MPPTaskPtr & task, const MPPTaskProxyPtr & task_proxytask_proxy);

    void unregisterTask(const MPPTaskId & task_id);

    MPPTaskProxyPtr findTaskWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout, std::string & errMsg);

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
};

} // namespace DB
