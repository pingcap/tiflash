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
    String toString() const {
        return "[" + std::to_string(start_ts) + "," + std::to_string(task_id) + "]";
    }
};


struct MPPTunnel
{
    std::mutex mu;
    std::condition_variable cv_for_connected;
    std::condition_variable cv_for_finished;

    bool connected; // if the exchange in has connected this tunnel.

    bool finished; // if the tunnel has finished its connection.

    ::grpc::ServerWriter<::mpp::MPPDataPacket> * writer;

    // tunnel id is in the format like "tunnel[sender]+[receiver]"
    String tunnel_id;

    Logger * log;

    MPPTunnel(const mpp::TaskMeta & receiver_meta_, const mpp::TaskMeta & sender_meta_)
        : connected(false),
          finished(false),
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

        LOG_DEBUG(log, "ready to write");
        std::unique_lock<std::mutex> lk(mu);

        // TODO: consider time constraining
        cv_for_connected.wait(lk, [&]() { return connected; });

        LOG_DEBUG(log, "begin to write");

        writer->Write(data);

        LOG_DEBUG(log, "finish write");
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

    void write(const std::string & data)
    {
        mpp::MPPDataPacket packet;
        packet.set_data(data);
        for (auto tunnel : tunnels)
        {
            tunnel->write(packet);
        }
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
};

using MPPTunnelSetPtr = std::shared_ptr<MPPTunnelSet>;

class MPPTaskManager;

struct MPPTask : private boost::noncopyable
{
    MPPTaskId id;

    mpp::TaskMeta meta;

    // worker handles the execution of task.
    std::thread worker;

    // which targeted task we should send data by which tunnel.
    std::map<MPPTaskId, MPPTunnelPtr> tunnel_map;

    MPPTaskManager * manager;

    Logger * log;

    Exception err;

    MPPTask(const mpp::TaskMeta & meta_) : meta(meta_), log(&Logger::get("task " + std::to_string(meta_.task_id())))
    {
        id.start_ts = meta.start_ts();
        id.task_id = meta.task_id();
    }

    ~MPPTask() { worker.join(); }

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

    void finishWrite() {
        for (auto it : tunnel_map) {
            it.second->writeDone();
        }
    }

    void run(BlockIO io)
    {
        worker = std::thread(&MPPTask::runImpl, this, io);
    }

    void registerTunnel(const MPPTaskId & id, MPPTunnelPtr tunnel) { tunnel_map[id] = tunnel; }

    MPPTunnelPtr getTunnel(const mpp::TaskMeta & meta)
    {
        MPPTaskId id{meta.start_ts(), meta.task_id()};
        const auto & it = tunnel_map.find(id);
        if (it == tunnel_map.end())
        {
            return nullptr;
        }
        return it->second;
    }
};

using MPPTaskPtr = std::shared_ptr<MPPTask>;

// MPPTaskManger holds all running mpp tasks. It's a single instance holden in Context.
class MPPTaskManager : private boost::noncopyable
{
    std::mutex mu;

    std::map<MPPTaskId, MPPTaskPtr> task_map;

    Logger * log;

public:
    MPPTaskManager() : log(&Logger::get("TaskManager")) {}

    void registerTask(MPPTaskPtr task)
    {
        std::lock_guard<std::mutex> lock(mu);
        if (task_map.find(task->id) != task_map.end())
        {
            throw Exception("The task " + task->id.toString() + " has been registered");
        }
        task_map.emplace(task->id, task);
        task->manager = this;
    }

    void unregisterTask(MPPTask* task) {
        std::lock_guard<std::mutex> lock(mu);
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

    MPPTaskPtr findTask(const mpp::TaskMeta & meta)
    {
        std::lock_guard<std::mutex> lock(mu);
        MPPTaskId id{meta.start_ts(), meta.task_id()};
        const auto & it = task_map.find(id);
        if (it == task_map.end())
        {
            LOG_ERROR(log, "don't find task " << std::to_string(meta.task_id()) << " map have " << toString());
            return nullptr;
        }
        return it->second;
    }

    String toString() {
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