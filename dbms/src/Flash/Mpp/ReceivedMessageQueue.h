// Copyright 2023 PingCAP, Ltd.
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
#include <Common/LooseBoundedMPMCQueue.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <Flash/Mpp/ReceivedMessage.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>

#include <memory>

namespace DB
{
using ReceivedMessagePtr = std::shared_ptr<ReceivedMessage>;
using MsgChannelPtr = std::shared_ptr<LooseBoundedMPMCQueue<std::shared_ptr<ReceivedMessage>>>;

enum class ReceiverMode
{
    Local = 0,
    Sync,
    Async
};

class ReceivedMessageQueue
{
    /// msg_channel is a bounded queue that saves the received messages
    /// msg_channels_for_fine_grained_shuffle is unbounded queues that saves fine grained received messages
    /// all the received messages in msg_channels_for_fine_grained_shuffle must be saved in msg_channel first, so the
    /// total size of `ReceivedMessageQueue` is still under control even if msg_channels_for_fine_grained_shuffle
    /// is unbounded queues
    /// for non fine grained shuffle, all the read/write to the queue is based on msg_channel/grpc_recv_queue
    /// for fine grained shuffle
    /// write: the writer first write the msg to msg_channel/grpc_recv_queue, if write success, then write msg to msg_channels_for_fine_grained_shuffle
    /// read: the reader read msg from msg_channels_for_fine_grained_shuffle, and reduce the `remaining_consumers` in msg, if `remaining_consumers` is 0, then
    ///       remove the msg from msg_channel/grpc_recv_queue
    std::vector<MsgChannelPtr> msg_channels_for_fine_grained_shuffle;
    MsgChannelPtr msg_channel;
    std::shared_ptr<GRPCReceiveQueue<ReceivedMessagePtr>> grpc_recv_queue;
    size_t fine_grained_channel_size;
    LoggerPtr log;

public:
    template <bool need_wait>
    std::pair<MPMCQueueResult, ReceivedMessagePtr> pop(size_t stream_id);

    template <bool is_force>
    bool pushToMessageChannel(ReceivedMessagePtr & received_message, ReceiverMode mode);

    GRPCReceiveQueueRes pushToGRPCReceiveQueue(ReceivedMessagePtr & received_message);

    ReceivedMessageQueue(
        const AsyncRequestHandlerWaitQueuePtr & conn_wait_queue,
        const LoggerPtr & log_,
        const CapacityLimits & queue_limits,
        bool enable_fine_grained,
        size_t fine_grained_channel_size_);

    void finish()
    {
        grpc_recv_queue->finish();
        /// msg_channels_for_fine_grained_shuffle must be finished after msg_channel is finished
        for (auto & channel : msg_channels_for_fine_grained_shuffle)
            channel->finish();
    }
    void cancel()
    {
        grpc_recv_queue->cancel();
        /// msg_channels_for_fine_grained_shuffle must be cancelled after msg_channel is cancelled
        for (auto & channel : msg_channels_for_fine_grained_shuffle)
            channel->cancel();
    }
    size_t getFineGrainedStreamSize() const
    {
        return fine_grained_channel_size;
    }
    bool isWritable() const
    {
        return !msg_channel->isFull();
    }
};
} // namespace DB
