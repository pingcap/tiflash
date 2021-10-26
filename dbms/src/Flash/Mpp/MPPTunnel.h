#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <grpcpp/server_context.h>
#include <kvproto/mpp.pb.h>
#include <kvproto/tikvpb.grpc.pb.h>

#include <boost/noncopyable.hpp>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace DB
{
template <typename Writer>
class MPPTunnelBase : private boost::noncopyable
{
public:
    using TaskCancelledCallback = std::function<bool()>;

    MPPTunnelBase(
        const mpp::TaskMeta & receiver_meta_,
        const mpp::TaskMeta & sender_meta_,
        const std::chrono::seconds timeout_,
        TaskCancelledCallback callback,
        int input_steams_num_);

    ~MPPTunnelBase();

    const String & id() const { return tunnel_id; }

    bool isTaskCancelled();

    // write a single packet to the tunnel, it will block if tunnel is not ready.
    void write(const mpp::MPPDataPacket & data, bool close_after_write = false);

    // finish the writing.
    void writeDone();

    /// close() finishes the tunnel, if the tunnel is connected already, it will
    /// write the error message to the tunnel, otherwise it just close the tunnel
    void close(const String & reason);

    // a MPPConn request has arrived. it will build connection by this tunnel;
    void connect(Writer * writer_);

    // wait until all the data has been transferred.
    void waitForFinish();

//private:
    void waitUntilConnectedOrCancelled(std::unique_lock<std::mutex> & lk);

    // must under mu's protection
    void finishWithLock();

    /// to avoid being blocked when pop(), we should send nullptr into send_queue
    void sendLoop();

    std::mutex mu;
    std::mutex end_mu;
    std::condition_variable cv_for_connected;
    std::condition_variable cv_for_finished;
    std::condition_variable end_cv;
    std::atomic<bool> send_end;

    bool connected; // if the exchange in has connected this tunnel.

    std::atomic<bool> finished; // if the tunnel has finished its connection.

    Writer * writer;

    std::chrono::seconds timeout;

    TaskCancelledCallback task_cancelled_callback;

    // tunnel id is in the format like "tunnel[sender]+[receiver]"
    String tunnel_id;

    String send_loop_msg;

    int input_streams_num;

    std::unique_ptr<std::thread> send_thread;

    using MPPDataPacketPtr = std::shared_ptr<mpp::MPPDataPacket>;
    ConcurrentBoundedQueue<MPPDataPacketPtr> send_queue;

    Poco::Logger * log;
};

class MPPTunnel : public MPPTunnelBase<::grpc::ServerWriter<::mpp::MPPDataPacket>>
{
public:
    using Base = MPPTunnelBase<::grpc::ServerWriter<::mpp::MPPDataPacket>>;
    using Base::Base;
};

using MPPTunnelPtr = std::shared_ptr<MPPTunnel>;

} // namespace DB
