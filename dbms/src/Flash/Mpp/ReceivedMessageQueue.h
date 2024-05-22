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

#include <Common/FailPoint.h>
#include <Common/GRPCQueue.h>
#include <Common/LooseBoundedMPMCQueue.h>
#include <Common/PODArray.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Mpp/ReceivedMessage.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>

#include <memory>
#include <utility>

namespace DB
{
namespace ExchangeReceiverMetric
{
inline void addDataSizeMetric(std::atomic<Int64> & data_size_in_queue, size_t size)
{
    data_size_in_queue.fetch_add(size);
    GET_METRIC(tiflash_exchange_queueing_data_bytes, type_receive).Increment(size);
}

inline void subDataSizeMetric(std::atomic<Int64> & data_size_in_queue, size_t size)
{
    data_size_in_queue.fetch_sub(size);
    GET_METRIC(tiflash_exchange_queueing_data_bytes, type_receive).Decrement(size);
}

inline void clearDataSizeMetric(std::atomic<Int64> & data_size_in_queue)
{
    GET_METRIC(tiflash_exchange_queueing_data_bytes, type_receive).Decrement(data_size_in_queue.load());
}
} // namespace ExchangeReceiverMetric

using ReceivedMessagePtr = std::shared_ptr<ReceivedMessage>;

enum class ReceiverMode
{
    Local = 0,
    Sync,
    Async
};

class GRPCNotifyQueue : public NotifyFuture
{
public:
    template <typename... Args>
    explicit GRPCNotifyQueue(const LoggerPtr & log_, Args &&... args)
        : queue(log_, std::forward<Args>(args)...)
    {}

    void registerTask(TaskPtr && task) override { queue.registerPipeReadTask(std::move(task)); }

    MPMCQueueResult pop(ReceivedMessagePtr & data) { return queue.pop(data); }

    MPMCQueueResult tryPop(ReceivedMessagePtr & data) { return queue.tryPop(data); }

    MPMCQueueResult forcePush(ReceivedMessagePtr && data) { return queue.forcePush(std::move(data)); }

    MPMCQueueResult push(ReceivedMessagePtr && data) { return queue.push(std::move(data)); }

    MPMCQueueResult tryDequeue() { return queue.tryDequeue(); }

    MPMCQueueResult pushWithTag(ReceivedMessagePtr && data, GRPCKickTag * new_tag)
    {
        return queue.pushWithTag(std::move(data), new_tag);
    }

    void setKickFuncForTest(GRPCKickFunc && func) { queue.setKickFuncForTest(std::move(func)); }

    bool finish() { return queue.finish(); }
    bool cancel() { return queue.cancel(); }

    bool isWritable() const { return queue.isWritable(); }

    void registerPipeWriteTask(TaskPtr && task) { queue.registerPipeWriteTask(std::move(task)); }

private:
    GRPCRecvQueue<ReceivedMessagePtr> queue;
};

class MSGChannel : public NotifyFuture
{
public:
    void registerTask(TaskPtr && task) override { queue_ref.registerPipeReadTask(std::move(task)); }

    MPMCQueueResult pop(ReceivedMessagePtr & data) { return queue_ref.pop(data); }

    MPMCQueueResult tryPop(ReceivedMessagePtr & data) { return queue_ref.tryPop(data); }

    MPMCQueueResult forcePush(const ReceivedMessagePtr & data) { return queue_ref.forcePush(data); }

    bool finish() { return queue_ref.finish(); }
    bool cancel() { return queue_ref.cancel(); }

private:
    using QueueImpl = LooseBoundedMPMCQueue<ReceivedMessagePtr>;
    // these are unbounded queues.
    std::shared_ptr<QueueImpl> queue = std::make_shared<QueueImpl>(std::numeric_limits<size_t>::max());
    QueueImpl & queue_ref = *queue;
};

class ReceivedMessageQueue
{
public:
    ReceivedMessageQueue(
        const CapacityLimits & queue_limits,
        const LoggerPtr & log_,
        std::atomic<Int64> * data_size_in_queue_,
        bool enable_fine_grained,
        size_t fine_grained_channel_size_);

    template <bool need_wait>
    MPMCQueueResult pop(size_t stream_id, ReceivedMessagePtr & recv_msg);

    template <bool is_force>
    bool pushPacket(
        size_t source_index,
        const String & req_info,
        const TrackedMppDataPacketPtr & tracked_packet,
        ReceiverMode mode);

    MPMCQueueResult pushAsyncGRPCPacket(
        size_t source_index,
        const String & req_info,
        const TrackedMppDataPacketPtr & tracked_packet,
        GRPCKickTag * new_tag);

    void finish()
    {
        grpc_recv_queue.finish();
        /// msg_channels_for_fine_grained_shuffle must be finished after msg_channel is finished
        for (auto & channel : msg_channels_for_fine_grained_shuffle)
            channel.finish();
    }

    void cancel()
    {
        grpc_recv_queue.cancel();
        /// msg_channels_for_fine_grained_shuffle must be cancelled after msg_channel is cancelled
        for (auto & channel : msg_channels_for_fine_grained_shuffle)
            channel.cancel();
    }

    bool isWritable() const { return grpc_recv_queue.isWritable(); }

    void registerPipeWriteTask(TaskPtr && task) { grpc_recv_queue.registerPipeWriteTask(std::move(task)); }

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    const LoggerPtr log;
    std::atomic<Int64> * data_size_in_queue;
    const size_t fine_grained_channel_size;
    /// grpc_recv_queue is a bounded queue that saves the received messages
    /// msg_channels_for_fine_grained_shuffle is unbounded queues that saves fine grained received messages
    /// all the received messages in msg_channels_for_fine_grained_shuffle must be saved in msg_channel first, so the
    /// total size of `ReceivedMessageQueue` is still under control even if msg_channels_for_fine_grained_shuffle
    /// is unbounded queues
    /// for non fine grained shuffle, all the read/write to the queue is based on msg_channel/grpc_recv_queue
    /// for fine grained shuffle
    /// write: the writer first write the msg to msg_channel/grpc_recv_queue, if write success, then write msg to msg_channels_for_fine_grained_shuffle
    /// read: the reader read msg from msg_channels_for_fine_grained_shuffle, and reduce the `remaining_consumers` in msg, if `remaining_consumers` is 0, then
    ///       remove the msg from msg_channel/grpc_recv_queue
    std::vector<MSGChannel> msg_channels_for_fine_grained_shuffle;
    GRPCNotifyQueue grpc_recv_queue;
};

} // namespace DB
