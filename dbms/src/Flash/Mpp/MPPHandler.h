#pragma once

#include <DataStreams/BlockIO.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>
#include <condition_variable>
#include <mutex>
#include <thread>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
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


struct MPPTunnel
{
    std::mutex mu;
    std::condition_variable cv_for_connected;
    std::condition_variable cv_for_finished;

    bool connected; // if the exchange in has connected this tunnel.

    bool finished; // if the tunnel has finished its connection.

    ::grpc::ServerWriter<::mpp::MPPDataPacket> * writer;

    std::chrono::seconds timeout;

    // tunnel id is in the format like "tunnel[sender]+[receiver]"
    String tunnel_id;

    Logger * log;

    MPPTunnel(const mpp::TaskMeta & receiver_meta_, const mpp::TaskMeta & sender_meta_, const std::chrono::seconds timeout_)
        : connected(false),
          finished(false),
          timeout(timeout_),
          tunnel_id("tunnel" + std::to_string(sender_meta_.task_id()) + "+" + std::to_string(receiver_meta_.task_id())),
          log(&Logger::get(tunnel_id))
    {}

    ~MPPTunnel()
    {
        if (!finished)
            writeDone();
    }

    // write a single packet to the tunnel, it will block if tunnel is not ready.
    // TODO: consider to hold a buffer
    void write(const mpp::MPPDataPacket & data)
    {

        LOG_TRACE(log, "ready to write");
        std::unique_lock<std::mutex> lk(mu);

        if (timeout.count() > 0)
        {
            if (cv_for_connected.wait_for(lk, timeout, [&]() { return connected; }))
            {
                writer->Write(data);

                LOG_TRACE(log, "finish write");
            }
            else
            {
                throw Exception(tunnel_id + " is timeout");
            }
        }
        else
        {
            cv_for_connected.wait(lk, [&]() { return connected; });
        }
    }

    // finish the writing.
    void writeDone()
    {
        std::lock_guard<std::mutex> lk(mu);
        if (finished)
            throw Exception("has finished");
        finished = true;
        cv_for_finished.notify_all();
    }

    // a MPPConn request has arrived. it will build connection by this tunnel;
    void connect(::grpc::ServerWriter<::mpp::MPPDataPacket> * writer_)
    {
        if (connected)
        {
            throw Exception("has connected");
        }
        std::lock_guard<std::mutex> lk(mu);
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

    // this is a broadcast writing.
    void write(const std::string & data)
    {
        mpp::MPPDataPacket packet;
        packet.set_data(data);
        for (auto tunnel : tunnels)
        {
            tunnel->write(packet);
        }
    }

    // this is a partition writing.
    void write(const std::string & data, int16_t partition_id)
    {
        mpp::MPPDataPacket packet;
        packet.set_data(data);
        tunnels[partition_id]->write(packet);
    }
    void writeError(mpp::Error err)
    {
        mpp::MPPDataPacket packet;
        packet.set_allocated_error(&err);
        for (auto tunnel : tunnels)
        {
            tunnel->write(packet);
        }
    }

    uint16_t getPartitionNum() { return tunnels.size(); }
};

using MPPTunnelSetPtr = std::shared_ptr<MPPTunnelSet>;

class MPPTaskManager;

struct MPPTask : private boost::noncopyable
{
    MPPTaskId id;

    mpp::TaskMeta meta;

    // which targeted task we should send data by which tunnel.
    std::map<MPPTaskId, MPPTunnelPtr> tunnel_map;

    MPPTaskManager * manager;

    Logger * log;

    Exception err;

    std::condition_variable cv;

    MPPTask(const mpp::TaskMeta & meta_) : meta(meta_), log(&Logger::get("task " + std::to_string(meta_.task_id())))
    {
        id.start_ts = meta.start_ts();
        id.task_id = meta.task_id();
    }

    void unregisterTask();

    void runImpl(BlockIO io);

    void writeErrToAllTunnel(const String & e)
    {
        for (auto it : tunnel_map)
        {
            mpp::MPPDataPacket data;
            auto err = new mpp::Error();
            err->set_msg(e);
            data.set_allocated_error(err);
            it.second->write(data);
            it.second->writeDone();
        }
    }

    void finishWrite()
    {
        for (auto it : tunnel_map)
        {
            it.second->writeDone();
        }
    }

    void run(BlockIO io)
    {
        std::thread worker(&MPPTask::runImpl, this, io);
        worker.detach();
    }

    std::mutex tunnel_mutex;

    void registerTunnel(const MPPTaskId & id, MPPTunnelPtr tunnel)
    {
        std::unique_lock<std::mutex> lk(tunnel_mutex);
        if (tunnel_map.find(id) != tunnel_map.end())
        {
            throw Exception("the tunnel " + tunnel->tunnel_id + " has been registered");
        }
        tunnel_map[id] = tunnel;
        cv.notify_all();
    }

    MPPTunnelPtr getTunnelWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout)
    {
        MPPTaskId id{meta.start_ts(), meta.task_id()};
        std::map<MPPTaskId, MPPTunnelPtr>::iterator it;
        std::unique_lock<std::mutex> lk(tunnel_mutex);
        auto ret = cv.wait_for(lk, timeout, [&] {
            it = tunnel_map.find(id);
            return it == tunnel_map.end();
        });
        return ret ? nullptr : it->second;
    }
};

using MPPTaskPtr = std::shared_ptr<MPPTask>;

// MPPTaskManger holds all running mpp tasks. It's a single instance holden in Context.
class MPPTaskManager : private boost::noncopyable
{
    std::mutex mu;

    std::map<MPPTaskId, MPPTaskPtr> task_map;

    Logger * log;

    std::condition_variable cv;

public:
    MPPTaskManager() : log(&Logger::get("TaskManager")) {}

    void registerTask(MPPTaskPtr task)
    {
        std::unique_lock<std::mutex> lock(mu);
        if (task_map.find(task->id) != task_map.end())
        {
            throw Exception("The task " + task->id.toString() + " has been registered");
        }
        task_map.emplace(task->id, task);
        task->manager = this;
        cv.notify_all();
    }

    void unregisterTask(MPPTask * task)
    {
        std::unique_lock<std::mutex> lock(mu);
        auto it = task_map.find(task->id);
        if (it != task_map.end())
        {
            task_map.erase(it);
        }
        else
        {
            LOG_ERROR(log, "The task " + task->id.toString() + " cannot be found and fail to unregister");
        }
    }

    MPPTaskPtr findTaskWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout)
    {
        MPPTaskId id{meta.start_ts(), meta.task_id()};
        std::map<MPPTaskId, MPPTaskPtr>::iterator it;
        std::unique_lock<std::mutex> lock(mu);
        auto ret = cv.wait_for(lock, timeout, [&] {
            it = task_map.find(id);
            return it == task_map.end();
        });
        return ret ? nullptr : it->second;
    }

    String toString()
    {
        std::lock_guard<std::mutex> lock(mu);
        String res;
        for (auto it : task_map)
        {
            res += "(" + it.first.toString() + ", ";
        }
        return res + ")";
    }
};

class MPPHandler
{
    Context & context;
    const mpp::DispatchTaskRequest & task_request;

    Logger * log;

public:
    MPPHandler(Context & context_, const mpp::DispatchTaskRequest & task_request_)
        : context(context_), task_request(task_request_), log(&Logger::get("MPPHandler"))
    {}
    grpc::Status execute(mpp::DispatchTaskResponse * response);
};

} // namespace DB
