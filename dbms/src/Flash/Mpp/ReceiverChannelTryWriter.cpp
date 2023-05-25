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

#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <Flash/Mpp/ReceiverChannelTryWriter.h>

#include <utility>

namespace DB
{
template <bool enable_fine_grained_shuffle>
GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWrite(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
{
    auto received_message = toReceivedMessage(tracked_packet, source_index, req_info, enable_fine_grained_shuffle, fine_grained_channel_size);
    if (!received_message->containUsefulMessage())
    {
        /// don't need to push an empty response to received_message_queue
        return GRPCReceiveQueueRes::OK;
    }

    GRPCReceiveQueueRes res;
    res = tryWriteImpl<enable_fine_grained_shuffle>(received_message);
    fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, res = GRPCReceiveQueueRes::CANCELLED);

    if (likely(res == GRPCReceiveQueueRes::OK || res == GRPCReceiveQueueRes::FULL))
        ExchangeReceiverMetric::addDataSizeMetric(*data_size_in_queue, tracked_packet->getPacket().ByteSizeLong());
    LOG_TRACE(log, "push recv_msg to msg_channel, res:{}, enable_fine_grained_shuffle: {}, fine grained channel size: {}", magic_enum::enum_name(res), enable_fine_grained_shuffle, fine_grained_channel_size);
    return res;
}

template <bool enable_fine_grained_shuffle>
GRPCReceiveQueueRes ReceiverChannelTryWriter::tryReWrite()
{
    GRPCReceiveQueueRes res = GRPCReceiveQueueRes::OK;
    if (rewrite_msg != nullptr)
    {
        res = tryRewriteImpl<enable_fine_grained_shuffle>(rewrite_msg);
        if (res == GRPCReceiveQueueRes::OK)
        {
            rewrite_msg = nullptr;
        }
        /// if rewrite fails, wait for the next rewrite
    }
    fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, res = GRPCReceiveQueueRes::CANCELLED);

    return res;
}

template <bool enable_fine_grained_shuffle>
GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWriteImpl(ReceivedMessagePtr & msg)
{
    assert(rewrite_msg == nullptr);
    GRPCReceiveQueueRes res = received_message_queue->grpc_recv_queue->push(msg);
    if constexpr (enable_fine_grained_shuffle)
    {
        if (res == GRPCReceiveQueueRes::OK)
        {
            if (!writeMessageToFineGrainChannels(msg))
                res = GRPCReceiveQueueRes::CANCELLED;
        }
    }
    if (res == GRPCReceiveQueueRes::FULL)
        rewrite_msg = std::move(msg);
    return res;
}

template <bool enable_fine_grained_shuffle>
GRPCReceiveQueueRes ReceiverChannelTryWriter::tryRewriteImpl(ReceivedMessagePtr & msg)
{
    auto res = received_message_queue->grpc_recv_queue->push(msg);
    if constexpr (enable_fine_grained_shuffle)
    {
        if (res == GRPCReceiveQueueRes::OK)
        {
            /// if write to first queue success, then write the message to fine grain queues
            if (!writeMessageToFineGrainChannels(rewrite_msg))
                res = GRPCReceiveQueueRes::CANCELLED;
        }
    }
    return res;
}

template GRPCReceiveQueueRes ReceiverChannelTryWriter::tryReWrite<true>();
template GRPCReceiveQueueRes ReceiverChannelTryWriter::tryReWrite<false>();
template GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWrite<true>(size_t, const TrackedMppDataPacketPtr &);
template GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWrite<false>(size_t, const TrackedMppDataPacketPtr &);
template GRPCReceiveQueueRes ReceiverChannelTryWriter::tryRewriteImpl<true>(ReceivedMessagePtr &);
template GRPCReceiveQueueRes ReceiverChannelTryWriter::tryRewriteImpl<false>(ReceivedMessagePtr &);
template GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWriteImpl<true>(ReceivedMessagePtr &);
template GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWriteImpl<false>(ReceivedMessagePtr &);
} // namespace DB
