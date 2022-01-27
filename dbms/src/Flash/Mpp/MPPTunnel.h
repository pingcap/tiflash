#pragma once

#include <Common/LogWithPrefix.h>
#include <Common/MPMCQueue.h>
#include <Common/ThreadManager.h>
#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <common/logger_useful.h>
#include <common/types.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/server_context.h>
#include <kvproto/mpp.pb.h>
#include <kvproto/tikvpb.grpc.pb.h>
#pragma GCC diagnostic pop

#include <boost/noncopyable.hpp>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>

namespace DB
{
/**
 * MPPTunnelBase represents the sender of an exchange connection.
 *
 * (Deprecated) It is designed to be a template class so that we can mock a MPPTunnel without involving gRPC.
 *
 * The lifecycle of a MPPTunnel can be indicated by `connected` and `finished`:
 * | Stage                          | `connected` | `finished` |
 * |--------------------------------|-------------|------------|
 * | After constructed              | false       | false      |
 * | After `close` before `connect` | false       | true       |
 * | After `connect`                | true        | false      |
 * | After `consumerFinish`         | true        | true       |
 *
 * To be short: before `connect`, only `close` can finish a MPPTunnel; after `connect`, only `consumerFinish` can.
 *
 * Each MPPTunnel has a consumer to consume data. There're two kinds of consumers: local and remote.
 * - Remote consumer is owned by MPPTunnel itself. MPPTunnel will create a thread and run `sendLoop`.
 * - Local consumer is owned by the associated ExchangeReceiver (in the same process).
 * 
 * The protocol between MPPTunnel and consumer:
 * - All data will be pushed into the `send_queue`, including errors.
 * - MPPTunnel may close `send_queue` to notify consumer normally finish.
 * - Consumer may close `send_queue` to notify MPPTunnel that an error occurs.
 * - After `connect` only the consumer can set `finished` to `true`.
 * - Consumer's state is saved in `consumer_state` and be available after consumer finished.
 *
 * NOTE: to avoid deadlock, `waitForConsumerFinish` should be called outside of the protection of `mu`.
 */
template <typename Writer>
class MPPTunnelBase : private boost::noncopyable
{
public:
    MPPTunnelBase(
        const mpp::TaskMeta & receiver_meta_,
        const mpp::TaskMeta & sender_meta_,
        std::chrono::seconds timeout_,
        int input_steams_num_,
        bool is_local_,
        const LogWithPrefixPtr & log_ = nullptr);

    ~MPPTunnelBase();

    const String & id() const { return tunnel_id; }

    // write a single packet to the tunnel, it will block if tunnel is not ready.
    void write(const mpp::MPPDataPacket & data, bool close_after_write = false);

    // finish the writing.
    void writeDone();

    std::shared_ptr<mpp::MPPDataPacket> readForLocal();

    /// close() finishes the tunnel, if the tunnel is connected already, it will
    /// write the error message to the tunnel, otherwise it just close the tunnel
    void close(const String & reason);

    // a MPPConn request has arrived. it will build connection by this tunnel;
    void connect(Writer * writer_);

    // wait until all the data has been transferred.
    void waitForFinish();

    const ConnectionProfileInfo & getConnectionProfileInfo() const { return connection_profile_info; }

    bool isLocal() const { return is_local; }

    const LogWithPrefixPtr & getLogger() const { return log; }

    void consumerFinish(const String & err_msg);

private:
    void waitUntilConnectedOrFinished(std::unique_lock<std::mutex> & lk);

    void sendLoop();

    void waitForConsumerFinish(bool allow_throw);

    std::mutex mu;
    std::condition_variable cv_for_connected_or_finished;

    bool connected; // if the exchange in has connected this tunnel.

    bool finished; // if the tunnel has finished its connection.

    bool is_local; // if this tunnel used for local environment

    Writer * writer;

    std::chrono::seconds timeout;

    // tunnel id is in the format like "tunnel[sender]+[receiver]"
    String tunnel_id;

    int input_streams_num;

    using MPPDataPacketPtr = std::shared_ptr<mpp::MPPDataPacket>;
    MPMCQueue<MPPDataPacketPtr> send_queue;

    /// Consumer can be sendLoop or local receiver.
    class ConsumerState
    {
    public:
        ConsumerState()
            : future(promise.get_future())
        {
        }

        // before finished, must be called without protection of mu
        String getError()
        {
            future.wait();
            return future.get();
        }

        void setError(const String & err_msg)
        {
            promise.set_value(err_msg);
        }

    private:
        std::promise<String> promise;
        std::shared_future<String> future;
    };
    ConsumerState consumer_state;

    ConnectionProfileInfo connection_profile_info;

    const LogWithPrefixPtr log;
};

class MPPTunnel : public MPPTunnelBase<::grpc::ServerWriter<::mpp::MPPDataPacket>>
{
public:
    using Base = MPPTunnelBase<::grpc::ServerWriter<::mpp::MPPDataPacket>>;
    using Base::Base;
};

using MPPTunnelPtr = std::shared_ptr<MPPTunnel>;

} // namespace DB
