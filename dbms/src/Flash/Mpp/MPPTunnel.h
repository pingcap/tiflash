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

#include <Common/ConcurrentIOQueue.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadManager.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/GRPCSendQueue.h>
#include <Flash/Mpp/LocalRequestHandler.h>
#include <Flash/Mpp/PacketWriter.h>
#include <Flash/Mpp/ReceiverChannelWriter.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <common/StringRef.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/types.h>

#include <atomic>
#include <type_traits>


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

namespace MPPTunnelMetric
{
inline void addDataSizeMetric(std::atomic<Int64> & data_size_in_queue, size_t size)
{
    data_size_in_queue.fetch_add(size);
    GET_METRIC(tiflash_exchange_queueing_data_bytes, type_send).Increment(size);
}

inline void subDataSizeMetric(std::atomic<Int64> & data_size_in_queue, size_t size)
{
    data_size_in_queue.fetch_sub(size);
    GET_METRIC(tiflash_exchange_queueing_data_bytes, type_send).Decrement(size);
}

inline void clearDataSizeMetric(std::atomic<Int64> & data_size_in_queue)
{
    GET_METRIC(tiflash_exchange_queueing_data_bytes, type_send).Decrement(data_size_in_queue.load());
}
} // namespace MPPTunnelMetric

class IAsyncCallData;

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
    virtual ~TunnelSender() = default;
    TunnelSender(MemoryTrackerPtr & memory_tracker_, const LoggerPtr & log_, const String & tunnel_id_, std::atomic<Int64> * data_size_in_queue_)
        : memory_tracker(memory_tracker_)
        , log(log_)
        , tunnel_id(tunnel_id_)
        , data_size_in_queue(data_size_in_queue_)
    {
    }

    virtual bool push(TrackedMppDataPacketPtr &&) = 0;
    virtual bool nonBlockingPush(TrackedMppDataPacketPtr &&) = 0;

    virtual void cancelWith(const String &) = 0;

    virtual bool finish() = 0;

    virtual bool isReadyForWrite() const = 0;

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
    MemoryTracker * getMemoryTracker() const
    {
        return memory_tracker != nullptr ? memory_tracker.get() : nullptr;
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

    MemoryTrackerPtr memory_tracker;
    ConsumerState consumer_state;
    const LoggerPtr log;
    const String tunnel_id;

    std::atomic<Int64> * data_size_in_queue; // Come from MppTunnel
};

/// SyncTunnelSender maintains a new thread itself to consume and send data
class SyncTunnelSender : public TunnelSender
{
public:
    SyncTunnelSender(size_t queue_size, MemoryTrackerPtr & memory_tracker_, const LoggerPtr & log_, const String & tunnel_id_, std::atomic<Int64> * data_size_in_queue_)
        : TunnelSender(memory_tracker_, log_, tunnel_id_, data_size_in_queue_)
        , send_queue(ConcurrentIOQueue<TrackedMppDataPacketPtr>(queue_size))
    {}

    ~SyncTunnelSender() override;
    void startSendThread(PacketWriter * writer);

    bool push(TrackedMppDataPacketPtr && data) override
    {
        return send_queue.push(std::move(data)) == MPMCQueueResult::OK;
    }

    bool nonBlockingPush(TrackedMppDataPacketPtr && data) override
    {
        return send_queue.nonBlockingPush(std::move(data)) == MPMCQueueResult::OK;
    }

    void cancelWith(const String & reason) override
    {
        send_queue.cancelWith(reason);
    }

    bool finish() override
    {
        return send_queue.finish();
    }

    bool isReadyForWrite() const override
    {
        return !send_queue.isFull();
    }

private:
    friend class tests::TestMPPTunnel;
    void sendJob(PacketWriter * writer);
    std::shared_ptr<ThreadManager> thread_manager;
    ConcurrentIOQueue<TrackedMppDataPacketPtr> send_queue;
};

/// AsyncTunnelSender is mainly triggered by the Async PacketWriter which handles GRPC request/response in async mode, send one element one time
class AsyncTunnelSender : public TunnelSender
{
public:
    AsyncTunnelSender(size_t queue_size, MemoryTrackerPtr & memory_tracker, const LoggerPtr & log_, const String & tunnel_id_, grpc_call * call_, std::atomic<Int64> * data_size_in_queue)
        : TunnelSender(memory_tracker, log_, tunnel_id_, data_size_in_queue)
        , queue(queue_size, call_, log_)
    {}

    /// For gtest usage.
    AsyncTunnelSender(size_t queue_size, MemoryTrackerPtr & memoryTracker, const LoggerPtr & log_, const String & tunnel_id_, GRPCKickFunc func, std::atomic<Int64> * data_size_in_queue)
        : TunnelSender(memoryTracker, log_, tunnel_id_, data_size_in_queue)
        , queue(queue_size, func)
    {}

    bool push(TrackedMppDataPacketPtr && data) override
    {
        return queue.push(std::move(data));
    }

    bool nonBlockingPush(TrackedMppDataPacketPtr && data) override
    {
        return queue.nonBlockingPush(std::move(data));
    }

    bool finish() override
    {
        return queue.finish();
    }

    bool isReadyForWrite() const override
    {
        return !queue.isFull();
    }

    void cancelWith(const String & reason) override
    {
        queue.cancelWith(reason);
    }

    const String & getCancelReason() const
    {
        return queue.getCancelReason();
    }

    GRPCSendQueueRes pop(TrackedMppDataPacketPtr & data, void * new_tag)
    {
        return queue.pop(data, new_tag);
    }

    void subDataSizeMetric(size_t size)
    {
        ::DB::MPPTunnelMetric::subDataSizeMetric(*data_size_in_queue, size);
    }

private:
    GRPCSendQueue<TrackedMppDataPacketPtr> queue;
};

// local_only means ExhangeReceiver receives data only from local
template <bool enable_fine_grained_shuffle, bool local_only>
class LocalTunnelSenderV2 : public TunnelSender
{
public:
    LocalTunnelSenderV2(
        size_t source_index_,
        LocalRequestHandler & local_request_handler_,
        const LoggerPtr & log_,
        MemoryTrackerPtr & memory_tracker_,
        const String & tunnel_id_)
        : TunnelSender(memory_tracker_, log_, tunnel_id_, nullptr)
        , source_index(source_index_)
        , local_request_handler(local_request_handler_)
        , is_done(false)
    {
        local_request_handler.setAlive();
    }

    ~LocalTunnelSenderV2() override
    {
        RUNTIME_ASSERT(is_done, "Local tunnel is destructed before called by cancel() or finish()");

        // It should only be called in the destructor.
        //
        // This function is used to hold the destruction of receiver so that the push operation
        // of local tunnel is always valid(valid means pushing data to an alive reveiver).
        local_request_handler.closeConnection();
    }

    bool push(TrackedMppDataPacketPtr && data) override
    {
        return pushImpl<false>(std::move(data));
    }

    bool nonBlockingPush(TrackedMppDataPacketPtr && data) override
    {
        return pushImpl<true>(std::move(data));
    }

    void cancelWith(const String & reason) override
    {
        finishWrite(true, reason);
    }

    bool finish() override
    {
        finishWrite(false, "");
        return true;
    }

    bool isReadyForWrite() const override
    {
        if constexpr (local_only)
            return local_request_handler.isReadyForWrite();
        else
        {
            std::lock_guard lock(mu);
            return local_request_handler.isReadyForWrite();
        }
    }

private:
    friend class tests::TestMPPTunnel;

    template <bool non_blocking>
    bool pushImpl(TrackedMppDataPacketPtr && data)
    {
        if (unlikely(checkPacketErr(data)))
            return false;

        // receiver_mem_tracker pointer will always be valid because ExchangeReceiverBase won't be destructed
        // before all local tunnels are destructed so that the MPPTask which contains ExchangeReceiverBase and
        // is responsible for deleting receiver_mem_tracker must be destroyed after these local tunnels.
        data->switchMemTracker(local_request_handler.recv_mem_tracker);

        // When ExchangeReceiver receives data from local and remote tiflash, number of local tunnel threads
        // is very large and causes the time of transfering data by grpc threads becomes longer, because
        // grpc thread is hard to get chance to push data into MPMCQueue in ExchangeReceiver.
        // Adding a lock ensures that there is only one other thread competing with async reactor,
        // so the probability of async reactor getting the lock is 1/2.
        if constexpr (local_only)
            return local_request_handler.write<enable_fine_grained_shuffle, non_blocking>(source_index, data);
        else
        {
            std::lock_guard lock(mu);
            return local_request_handler.write<enable_fine_grained_shuffle, non_blocking>(source_index, data);
        }
    }

    bool checkPacketErr(TrackedMppDataPacketPtr & packet)
    {
        if (packet->hasError())
        {
            finishWrite(true, packet->error());
            return true;
        }
        return false;
    }

    // Need to tell receiver that the local tunnel will be closed and the receiver should
    // close channels otherwise the MPPTask may hang.
    void finishWrite(bool meet_error, const String & local_err_msg)
    {
        bool expect = false;
        if (is_done.compare_exchange_strong(expect, true))
        {
            consumer_state.setMsg(local_err_msg);
            local_request_handler.writeDone(meet_error, local_err_msg);
        }
    }

    size_t source_index;
    LocalRequestHandler local_request_handler;
    std::atomic_bool is_done;
    mutable std::mutex mu;
};

// TODO remove it in the future
class LocalTunnelSenderV1 : public TunnelSender
{
public:
    using Base = TunnelSender;
    using Base::Base;

    LocalTunnelSenderV1(size_t queue_size, MemoryTrackerPtr & memory_tracker_, const LoggerPtr & log_, const String & tunnel_id_, std::atomic<Int64> * data_size_in_queue_)
        : TunnelSender(memory_tracker_, log_, tunnel_id_, data_size_in_queue_)
        , send_queue(queue_size)
    {}

    TrackedMppDataPacketPtr readForLocal();

    bool push(TrackedMppDataPacketPtr && data) override
    {
        return send_queue.push(std::move(data)) == MPMCQueueResult::OK;
    }

    bool nonBlockingPush(TrackedMppDataPacketPtr && data) override
    {
        return send_queue.nonBlockingPush(std::move(data)) == MPMCQueueResult::OK;
    }

    void cancelWith(const String & reason) override
    {
        send_queue.cancelWith(reason);
    }

    bool finish() override
    {
        return send_queue.finish();
    }

    bool isReadyForWrite() const override
    {
        return !send_queue.isFull();
    }

private:
    bool cancel_reason_sent = false;
    ConcurrentIOQueue<TrackedMppDataPacketPtr> send_queue;
};

using TunnelSenderPtr = std::shared_ptr<TunnelSender>;
using SyncTunnelSenderPtr = std::shared_ptr<SyncTunnelSender>;
using AsyncTunnelSenderPtr = std::shared_ptr<AsyncTunnelSender>;
using LocalTunnelSenderV1Ptr = std::shared_ptr<LocalTunnelSenderV1>;
using LocalTunnelSenderV2Ptr = std::shared_ptr<LocalTunnelSenderV2<false, false>>;
using LocalTunnelFineGrainedSenderV2Ptr = std::shared_ptr<LocalTunnelSenderV2<true, false>>;
using LocalTunnelSenderLocalOnlyV2Ptr = std::shared_ptr<LocalTunnelSenderV2<false, true>>;
using LocalTunnelSenderFineGrainedLocalOnlyV2Ptr = std::shared_ptr<LocalTunnelSenderV2<true, true>>;

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

    // write a single packet to the tunnel's send queue, it will block if tunnel is not ready.
    void write(TrackedMppDataPacketPtr && data);

    // nonBlockingWrite write a single packet to the tunnel's send queue without blocking,
    // and need to call isReadForWrite first.
    // ```
    // while (!isReadyForWrite()) {}
    // nonBlockingWrite(std::move(data));
    // ```
    void nonBlockingWrite(TrackedMppDataPacketPtr && data);
    bool isReadyForWrite() const;

    // finish the writing, and wait until the sender finishes.
    void writeDone();

    /// close() cancel the tunnel's send queue with `reason`, if reason is not empty, the tunnel sender will
    /// write this reason as an error message to its receiver. If `wait_sender_finish` is true, close() will
    /// not return until tunnel sender finishes, otherwise, close() will return just after the send queue is
    /// cancelled(which is a non-blocking operation)
    void close(const String & reason, bool wait_sender_finish);

    // a MPPConn request has arrived. it will build connection by this tunnel;
    void connectSync(PacketWriter * writer);

    void connectLocalV2(
        size_t source_index,
        LocalRequestHandler & local_request_handler,
        bool is_fine_grained,
        bool has_remote_conn);

    // like `connect` but it's intended to connect async grpc.
    void connectAsync(IAsyncCallData * data);

    void connectLocalV1(PacketWriter * writer);

    // wait until all the data has been transferred.
    void waitForFinish();

    const ConnectionProfileInfo & getConnectionProfileInfo() const { return connection_profile_info; }

    bool isLocal() const { return mode == TunnelSenderMode::LOCAL; }
    bool isAsync() const { return mode == TunnelSenderMode::ASYNC_GRPC; }

    const LoggerPtr & getLogger() const { return log; }

    TunnelSenderPtr getTunnelSender() { return tunnel_sender; }
    SyncTunnelSenderPtr getSyncTunnelSender() { return sync_tunnel_sender; }
    AsyncTunnelSenderPtr getAsyncTunnelSender() { return async_tunnel_sender; }
    LocalTunnelSenderV1Ptr getLocalTunnelSenderV1() { return local_tunnel_sender_v1; }

    LocalTunnelSenderV2Ptr getLocalTunnelSenderV2() { return local_tunnel_v2; }
    LocalTunnelFineGrainedSenderV2Ptr getLocalTunnelFineGrainedSenderV2() { return local_tunnel_fine_grained_v2; }
    LocalTunnelSenderLocalOnlyV2Ptr getLocalTunnelLocalOnlyV2() { return local_tunnel_local_only_v2; }
    LocalTunnelSenderFineGrainedLocalOnlyV2Ptr getLocalTunnelFineGrainedLocalOnlyV2() { return local_tunnel_fine_grained_local_only_v2; }

private:
    friend class tests::TestMPPTunnel;
    // TODO(hyb): Extract Cancelled status from Finished to distinguish Completed and Cancelled situation
    enum class TunnelStatus
    {
        Unconnected, // Not connect to any writer, not able to accept new data
        Connected, // Connected to some writer, accepting data
        WaitingForSenderFinish, // Wait for sender to finish
        Finished // Final state, no more work to do
    };

    std::string_view statusToString();

    void waitUntilConnectedOrFinished(std::unique_lock<std::mutex> & lk);

    void waitForSenderFinish(bool allow_throw);

    MemoryTracker * getMemTracker()
    {
        return mem_tracker ? mem_tracker.get() : nullptr;
    }

    void updateConnProfileInfo(size_t pushed_data_size)
    {
        std::lock_guard lock(mu);
        connection_profile_info.bytes += pushed_data_size;
        connection_profile_info.packets += 1;
    }

private:
    mutable std::mutex mu;
    std::condition_variable cv_for_status_changed;

    TunnelStatus status;

    std::chrono::seconds timeout;
    UInt64 timeout_nanoseconds{0};
    mutable std::optional<Stopwatch> timeout_stopwatch;

    // tunnel id is in the format like "tunnel[sender]+[receiver]"
    String tunnel_id;

    std::shared_ptr<MemoryTracker> mem_tracker;
    const size_t queue_size;
    ConnectionProfileInfo connection_profile_info;
    const LoggerPtr log;
    TunnelSenderMode mode; // Tunnel transfer data mode
    TunnelSenderPtr tunnel_sender; // Used to refer to one of sync/async/local_tunnel_sender which is not nullptr, just for coding convenience
    // According to mode value, among the sync/async/local_tunnel_senders, only the responding sender is not null and do actual work
    SyncTunnelSenderPtr sync_tunnel_sender;
    AsyncTunnelSenderPtr async_tunnel_sender;
    LocalTunnelSenderV1Ptr local_tunnel_sender_v1;

    LocalTunnelSenderV2Ptr local_tunnel_v2;
    LocalTunnelFineGrainedSenderV2Ptr local_tunnel_fine_grained_v2;
    LocalTunnelSenderLocalOnlyV2Ptr local_tunnel_local_only_v2;
    LocalTunnelSenderFineGrainedLocalOnlyV2Ptr local_tunnel_fine_grained_local_only_v2;
    std::atomic<Int64> data_size_in_queue;
};
using MPPTunnelPtr = std::shared_ptr<MPPTunnel>;

} // namespace DB
