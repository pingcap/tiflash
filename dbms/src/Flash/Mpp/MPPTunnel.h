#pragma once

#include <common/logger_useful.h>
#include <common/types.h>
#include <grpcpp/server_context.h>

#include <boost/noncopyable.hpp>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#include <kvproto/mpp.pb.h>
#include <kvproto/tikvpb.grpc.pb.h>
#pragma GCC diagnostic pop

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>

namespace DB
{

struct MPPTask;
class MPPTunnel : private boost::noncopyable
{
public:
    MPPTunnel(const mpp::TaskMeta & receiver_meta_,
        const mpp::TaskMeta & sender_meta_,
        const std::chrono::seconds timeout_,
        const std::shared_ptr<MPPTask> & current_task_);

    ~MPPTunnel();

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
    void connect(::grpc::ServerWriter<::mpp::MPPDataPacket> * writer_);

    // wait until all the data has been transferred.
    void waitForFinish();

private:
    void waitUntilConnectedOrCancelled(std::unique_lock<std::mutex> & lk);

    // must under mu's protection
    void finishWithLock();

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
};

using MPPTunnelPtr = std::shared_ptr<MPPTunnel>;

} // namespace DB
