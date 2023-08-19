// Copyright 2023 PingCAP, Inc.
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
class MPPTunnelTest;
class TestMPPTunnelBase;
} // namespace tests

class EstablishCallData;

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
        bool is_async_,
        const String & req_id);

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

    const LoggerPtr & getLogger() const { return log; }

    // do finish work for consumer, if need_lock is false, it means it has been protected by a mutex lock.
    void consumerFinish(const String & err_msg, bool need_lock = true);

    bool isSendQueueNextPopNonBlocking() { return send_queue.isNextPopNonBlocking(); }

    // In async mode, do a singe send operation when Writer::TryWrite() succeeds.
    // In sync mode, as a background task to keep sending until done.
    void sendJob(bool need_lock = true);

private:
    friend class tests::MPPTunnelTest;
    friend class tests::TestMPPTunnelBase;
    // For gtest usage
    MPPTunnelBase(
        const String & tunnel_id_,
        std::chrono::seconds timeout_,
        int input_steams_num_,
        bool is_local_,
        bool is_async_,
        const String & req_id);

    void finishSendQueue();

    void waitUntilConnectedOrFinished(std::unique_lock<std::mutex> & lk);

    void waitForConsumerFinish(bool allow_throw);

    std::mutex mu;
    std::condition_variable cv_for_connected_or_finished;

    bool connected; // if the exchange in has connected this tunnel.

    bool finished; // if the tunnel has finished its connection.

    bool is_local; // if the tunnel is used for local environment

    bool is_async; // if the tunnel is used for async server.

    Writer * writer;

    std::chrono::seconds timeout;

    // tunnel id is in the format like "tunnel[sender]+[receiver]"
    String tunnel_id;

    int input_streams_num;

    using MPPDataPacketPtr = std::shared_ptr<mpp::MPPDataPacket>;
    MPMCQueue<MPPDataPacketPtr> send_queue;

    std::shared_ptr<ThreadManager> thread_manager;

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
            err_has_set = true;
        }

        bool errHasSet() const
        {
            return err_has_set.load();
        }

    private:
        std::promise<String> promise;
        std::shared_future<String> future;
        std::atomic<bool> err_has_set{false};
    };
    ConsumerState consumer_state;

    ConnectionProfileInfo connection_profile_info;

    const LoggerPtr log;
};

class MPPTunnel : public MPPTunnelBase<PacketWriter>
{
public:
    using Base = MPPTunnelBase<PacketWriter>;
    using Base::Base;
};

using MPPTunnelPtr = std::shared_ptr<MPPTunnel>;

} // namespace DB
