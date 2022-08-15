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
class TestMPPTunnel;
} // namespace tests

class EstablishCallData;

enum class TunnelSenderMode
{
    SYNC_GRPC, // Using sync grpc writer
    LOCAL, // Expose internal memory access, no grpc writer needed
    ASYNC_GRPC // Using async grpc writer
};

/// TunnelSender is responsible for consuming data from Tunnel's internal send_queue and do the actual sending work
/// After TunnelSend finished its work, either normally or abnormally, set ConsumerState to inform Tunnel
class TunnelSender : private boost::noncopyable
{
public:
    using MPPDataPacketPtr = std::shared_ptr<mpp::MPPDataPacket>;
    using DataPacketMPMCQueuePtr = std::shared_ptr<MPMCQueue<MPPDataPacketPtr>>;
    virtual ~TunnelSender() = default;
    TunnelSender(TunnelSenderMode mode_, DataPacketMPMCQueuePtr send_queue_, PacketWriter * writer_, const LoggerPtr log_, const String & tunnel_id_)
        : mode(mode_)
        , send_queue(send_queue_)
        , writer(writer_)
        , log(log_)
        , tunnel_id(tunnel_id_)
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
    String getTunnelId()
    {
        return tunnel_id;
    }

protected:
    /// TunnelSender use consumer state to inform tunnel that whether sender has finished its work
    class ConsumerState
    {
    public:
        ConsumerState()
            : future(promise.get_future())
        {
        }
        String getMsg()
        {
            future.wait();
            return future.get();
        }
        void setMsg(const String & msg)
        {
            bool old_value = false;
            if (!msg_has_set.compare_exchange_strong(old_value, true, std::memory_order_seq_cst, std::memory_order_relaxed))
                return;
            promise.set_value(msg);
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
    const LoggerPtr log;
    String tunnel_id;
};

/// SyncTunnelSender maintains a new thread itself to consume and send data
class SyncTunnelSender : public TunnelSender
{
public:
    using Base = TunnelSender;
    using Base::Base;
    virtual ~SyncTunnelSender();
    void startSendThread();

private:
    friend class tests::TestMPPTunnel;
    void sendJob();
    std::shared_ptr<ThreadManager> thread_manager;
};

/// AsyncTunnelSender is mainly triggered by the Async PacketWriter which handles GRPC request/response in async mode, send one element one time
class AsyncTunnelSender : public TunnelSender
{
public:
    using Base = TunnelSender;
    using Base::Base;
    void tryFlushOne();
    void sendOne();
    bool isSendQueueNextPopNonBlocking() { return send_queue->isNextPopNonBlocking(); }
};

/// LocalTunnelSender just provide readForLocal method to return one element one time
/// LocalTunnelSender is owned by the associated ExchangeReceiver
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
 * MPPTunnel represents the sender of an exchange connection.
 *
 * The lifecycle of a MPPTunnel can be indicated by TunnelStatus:
 * | Previous Status        | Event           | New Status             |
 * |------------------------|-----------------|------------------------|
 * | NaN                    | Construction    | Unconnected            |
 * | Unconnected            | Close           | Finished               |
 * | Unconnected            | Connection      | Connected              |
 * | Connected              | WriteDone       | WaitingForSenderFinish |
 * | Connected              | Close           | WaitingForSenderFinish |
 * | Connected              | Encounter error | WaitingForSenderFinish |
 * | WaitingForSenderFinish | Sender Finished | Finished               |
 *
 * To be short: before connect, only close can finish a MPPTunnel; after connect, only Sender Finish can.
 *
 * Each MPPTunnel has a Sender to consume data. There're three kinds of senders: sync_remote, local and async_remote.
 * 
 * The protocol between MPPTunnel and Sender:
 * - All data will be pushed into the `send_queue`, including errors.
 * - MPPTunnel may finish `send_queue` to notify Sender normally finish.
 * - Sender may finish `send_queue` to notify MPPTunnel that an error occurs.
 * - After `status` turned to Connected only when Sender finish its work, MPPTunnel can set its 'status' to Finished.
 *
 * NOTE: to avoid deadlock, `waitForSenderFinish` should be called outside of the protection of `mu`.
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

    ~MPPTunnel();

    const String & id() const { return tunnel_id; }

    // write a single packet to the tunnel, it will block if tunnel is not ready.
    void write(const mpp::MPPDataPacket & data, bool close_after_write = false);

    // finish the writing.
    void writeDone();

    /// close() finishes the tunnel, if the tunnel is connected already, it will
    /// write the error message to the tunnel, otherwise it just close the tunnel
    void close(const String & reason);

    // a MPPConn request has arrived. it will build connection by this tunnel;
    void connect(PacketWriter * writer);

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
    friend class tests::TestMPPTunnel;
    // TODO(hyb): Extract Cancelled status from Finished to distinguish Completed and Cancelled situation
    enum class TunnelStatus
    {
        Unconnected, // Not connect to any writer, not able to accept new data
        Connected, // Connected to some writer, accepting data
        WaitingForSenderFinish, // Accepting all data already, wait for sender to finish
        Finished // Final state, no more work to do
    };

    StringRef statusToString();
    void finishSendQueue();

    void waitUntilConnectedOrFinished(std::unique_lock<std::mutex> & lk);

    void waitForSenderFinish(bool allow_throw);

    std::mutex mu;
    std::condition_variable cv_for_status_changed;

    TunnelStatus status;

    std::chrono::seconds timeout;

    // tunnel id is in the format like "tunnel[sender]+[receiver]"
    String tunnel_id;

    using MPPDataPacketPtr = std::shared_ptr<mpp::MPPDataPacket>;
    using DataPacketMPMCQueuePtr = std::shared_ptr<MPMCQueue<MPPDataPacketPtr>>;
    DataPacketMPMCQueuePtr send_queue;
    ConnectionProfileInfo connection_profile_info;
    const LoggerPtr log;
    TunnelSenderMode mode; // Tunnel transfer data mode
    TunnelSenderPtr tunnel_sender; // Used to refer to one of sync/async/local_tunnel_sender which is not nullptr, just for coding convenience
    // According to mode value, among the sync/async/local_tunnel_senders, only the responding sender is not null and do actual work
    SyncTunnelSenderPtr sync_tunnel_sender;
    AsyncTunnelSenderPtr async_tunnel_sender;
    LocalTunnelSenderPtr local_tunnel_sender;
};
using MPPTunnelPtr = std::shared_ptr<MPPTunnel>;

} // namespace DB
