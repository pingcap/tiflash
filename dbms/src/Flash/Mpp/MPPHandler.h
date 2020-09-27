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
};


struct MPPTunnel
{
    std::mutex mu;
    std::condition_variable cv_for_connected;
    std::condition_variable cv_for_finished;

    bool connected; // if the exchange in has connected this tunnel.

    bool finished; // if the tunnel has finished its connection.

    ::grpc::ServerWriter<::mpp::MPPDataPacket> * writer;

    Logger * log;

    MPPTunnel(const mpp::TaskMeta & client_meta_, const mpp::TaskMeta & server_meta_ ) : connected(false), finished(false),
        log(&Logger::get("tunnel" + std::to_string(server_meta_.task_id()) + "+" + std::to_string(client_meta_.task_id()))) {}

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

    ~MPPTunnelSet() { close(); }

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
    void close()
    {
        for (auto tunnel : tunnels)
        {
            tunnel->writeDone();
        }
    }
};

using MPPTunnelSetPtr = std::shared_ptr<MPPTunnelSet>;

struct MPPTask : private boost::noncopyable
{
    MPPTaskId id;

    mpp::TaskMeta meta;

    std::thread worker;

    std::map<MPPTaskId, MPPTunnelPtr> tunnel_map;

    Logger * log;

    Exception err;

    MPPTask(const mpp::TaskMeta & meta_) : log(&Logger::get("task " + std::to_string(meta_.task_id())))
    {
        meta = meta_;

        id.start_ts = meta.query_ts();
        id.task_id = meta.task_id();
    }

    ~MPPTask() { worker.join(); }

    void runImpl(BlockInputStreamPtr from, BlockOutputStreamPtr to)
    {
        try
        {
            LOG_DEBUG(log, "begin read prefix");
            from->readPrefix();
            to->writePrefix();
            LOG_DEBUG(log, "begin read ");

            while (Block block = from->read())
            {
                LOG_DEBUG(log, "write block " + std::to_string(block.rows()));
                to->write(block);
            }

            /// For outputting additional information in some formats.
            if (IProfilingBlockInputStream * input = dynamic_cast<IProfilingBlockInputStream *>(from.get()))
            {
                if (input->getProfileInfo().hasAppliedLimit())
                    to->setRowsBeforeLimit(input->getProfileInfo().getRowsBeforeLimit());

                to->setTotals(input->getTotals());
                to->setExtremes(input->getExtremes());
            }

            LOG_DEBUG(log, "end read ");

            from->readSuffix();
            to->writeSuffix();

            // TODO: Remove this task from task manager.
        }
        catch (Exception & e)
        {
            LOG_ERROR(log, "task running meets error " << e.displayText());
            err = e;
        }
    }

    void run(BlockIO io)
    {
        // TODO: Catch the exception and transfer errors to down stream.
        worker = std::thread(&MPPTask::runImpl, this, io.in, io.out);
    }

    void registerTunnel(const MPPTaskId & id, MPPTunnelPtr tunnel) { tunnel_map[id] = tunnel; }

    MPPTunnelPtr getTunnel(const mpp::TaskMeta & meta)
    {
        MPPTaskId id{meta.query_ts(), meta.task_id()};
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
    std::map<MPPTaskId, MPPTaskPtr> task_map;

public:
    void registerTask(MPPTaskPtr task) { task_map[task->id] = task; }

    MPPTaskPtr findTask(const mpp::TaskMeta & meta) const
    {
        MPPTaskId id{meta.query_ts(), meta.task_id()};
        const auto & it = task_map.find(id);
        if (it == task_map.end())
        {
            return nullptr;
        }
        return it->second;
    }
};

using MPPTaskManagerPtr = std::shared_ptr<MPPTaskManager>;

class MPPHandler
{
    Context & context;
    const mpp::DispatchTaskRequest & task_request;

    mpp::Error error;

    Logger * log;

public:
    MPPHandler(Context & context_, const mpp::DispatchTaskRequest & task_request_)
        : context(context_), task_request(task_request_), log(&Logger::get("MPPHandler"))
    {}
    grpc::Status execute(mpp::DispatchTaskResponse * response);
};

} // namespace DB