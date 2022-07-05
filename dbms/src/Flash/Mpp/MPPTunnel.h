// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/ThreadManager.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/PacketWriter.h>
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
namespace tests
{
class TestMPPTunnelBase;
} // namespace tests

class EstablishCallData;

enum class TunnelSenderMode
{
    SYNC_GRPC, // Using sync grpc writer
    LOCAL,     // Expose internal memory access, no grpc writer needed
    ASYNC_GRPC // Using async grpc writer
};

///
class TunnelSender : private boost::noncopyable
{
public:
    using MPPDataPacketPtr = std::shared_ptr<mpp::MPPDataPacket>;
    using DataPacketMPMCQueuePtr = std::shared_ptr<MPMCQueue<MPPDataPacketPtr>>;
    virtual ~TunnelSender() = default;
    TunnelSender(TunnelSenderMode mode_, DataPacketMPMCQueuePtr send_queue_, PacketWriter * writer_, std::shared_ptr<std::condition_variable> cv_for_status_changed_ptr_, const LoggerPtr log_)
            : mode(mode_)
            , send_queue(send_queue_)
            , writer(writer_)
            , cv_for_status_changed_ptr(cv_for_status_changed_ptr_)
            , log(log_)
    {
    }
    DataPacketMPMCQueuePtr getSendQueue()
    {
        return send_queue;
    }
    void consumerFinish(const String & err_msg);
    String getConsumerFinishMsg()
    {
        return consumer_state.getMsg();
    }
    bool isConsumerFinished()
    {
        return consumer_state.msgHasSet();
    }
    const LoggerPtr & getLogger() const { return log; }
protected:
    /// Consumer can be sendLoop or local receiver.
    class ConsumerState
    {
    public:
        ConsumerState()
                : future(promise.get_future())
        {
        }

        // before finished, must be called without protection of mu
        String getMsg()
        {
            future.wait();
            return future.get();
        }
        void setMsg(const String & msg)
        {
            promise.set_value(msg);
            msg_has_set = true;
        }
        bool msgHasSet() const
        {
            return msg_has_set.load();
        }
    private:
        std::promise<String> promise;
        std::shared_future<String> future;
        std::atomic<bool> msg_has_set{false};
    };
    TunnelSenderMode mode;
    DataPacketMPMCQueuePtr send_queue;
    ConsumerState consumer_state;
    PacketWriter * writer;
    std::shared_ptr<std::condition_variable> cv_for_status_changed_ptr;
    std::mutex state_mu;
    const LoggerPtr log;
};

class SyncTunnelSender : public TunnelSender
{
public:
    using Base = TunnelSender;
    using Base::Base;
    virtual ~SyncTunnelSender();
    void startSendThread();
private:
    friend class tests::TestMPPTunnelBase;
    void sendJob();
    std::shared_ptr<ThreadManager> thread_manager;
};

class AsyncTunnelSender : public TunnelSender
{
public:
    using Base = TunnelSender;
    using Base::Base;
    void tryFlushOne();
    void sendOne();
    bool isSendQueueNextPopNonBlocking() { return send_queue->isNextPopNonBlocking(); }
};

class LocalTunnelSender : public TunnelSender
{
public:
    using Base = TunnelSender;
    using Base::Base;
    MPPDataPacketPtr readForLocal();
};

using TunnelSenderPtr = std::shared_ptr<TunnelSender>;
using SyncTunnelSenderPtr = std::shared_ptr<SyncTunnelSender>;
using AsyncTunnelSenderPtr = std::shared_ptr<AsyncTunnelSender>;
using LocalTunnelSenderPtr = std::shared_ptr<LocalTunnelSender>;

/**
 * MPPTunnelBase represents the sender of an exchange connection.
 *
 * The lifecycle of a MPPTunnel can be indicated by `connected`, `finished` and tunnel_sender's consumerFinish:
 * | Stage                          | `connected` | `finished` | `tunnel_sender consumerFinish` |
 * |--------------------------------|-------------|------------|--------------------------------|
 * | After constructed              | false       | false      |     tunnel_sender is nullptr   |
 * | After `close` before `connect` | false       | true       |     tunnel_sender is nullptr   |
 * | After `connect`                | true        | false      |     consumerFinish is false    |
 * | After `consumerFinish`         | true        | unchanged       |
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
class MPPTunnel : private boost::noncopyable
{
public:
    MPPTunnel(
        const mpp::TaskMeta & receiver_meta_,
        const mpp::TaskMeta & sender_meta_,
        std::chrono::seconds timeout_,
        int input_steams_num_,
        bool is_local_,
        bool is_async_,
        const String & req_id);

    // For gtest usage
    MPPTunnel(
        const String & tunnel_id_,
        std::chrono::seconds timeout_,
        int input_steams_num_,
        bool is_local_,
        bool is_async_,
        const String & req_id);

    virtual ~MPPTunnel();

    const String & id() const { return tunnel_id; }

    // write a single packet to the tunnel, it will block if tunnel is not ready.
    void write(const mpp::MPPDataPacket & data, bool close_after_write = false);

    // finish the writing.
    void writeDone();

    /// close() finishes the tunnel, if the tunnel is connected already, it will
    /// write the error message to the tunnel, otherwise it just close the tunnel
    void close(const String & reason);

    // a MPPConn request has arrived. it will build connection by this tunnel;
    virtual void connect(PacketWriter * writer);

    // wait until all the data has been transferred.
    void waitForFinish();

    const ConnectionProfileInfo & getConnectionProfileInfo() const { return connection_profile_info; }

    bool isLocal() const { return mode == TunnelSenderMode::LOCAL; }
    bool isAsync() const { return mode == TunnelSenderMode::ASYNC_GRPC; }

    const LoggerPtr & getLogger() const { return log; }

    TunnelSenderPtr getTunnelSender() { return tunnel_sender; }
    SyncTunnelSenderPtr getSyncTunnelSender() { return sync_tunnel_sender; }
    AsyncTunnelSenderPtr getAsyncTunnelSender() { return async_tunnel_sender; }
    LocalTunnelSenderPtr getLocalTunnelSender() { return local_tunnel_sender; }
private:
    friend class tests::TestMPPTunnelBase;

    void finishSendQueue();

    void waitUntilConnectedOrFinished(std::unique_lock<std::mutex> & lk);

    void waitForConsumerFinish(bool allow_throw);

    std::mutex mu;
    std::shared_ptr<std::condition_variable> cv_for_status_changed_ptr;

    bool connected; // if the exchange in has connected this tunnel.

    bool finished; // if the tunnel has finished its connection.

    TunnelSenderMode mode; // Tunnel transfer data mode

    std::chrono::seconds timeout;

    // tunnel id is in the format like "tunnel[sender]+[receiver]"
    String tunnel_id;

    using MPPDataPacketPtr = std::shared_ptr<mpp::MPPDataPacket>;
    using DataPacketMPMCQueuePtr = std::shared_ptr<MPMCQueue<MPPDataPacketPtr>>;
    DataPacketMPMCQueuePtr send_queue;
    ConnectionProfileInfo connection_profile_info;
    const LoggerPtr log;
    TunnelSenderPtr tunnel_sender;
    SyncTunnelSenderPtr sync_tunnel_sender;
    AsyncTunnelSenderPtr async_tunnel_sender;
    LocalTunnelSenderPtr local_tunnel_sender;
};
using MPPTunnelPtr = std::shared_ptr<MPPTunnel>;

} // namespace DB
