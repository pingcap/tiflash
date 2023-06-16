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
GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWrite(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
{
    auto received_message = toReceivedMessage(tracked_packet, source_index, req_info, enable_fine_grained_shuffle, fine_grained_channel_size);
    if (!received_message->containUsefulMessage())
    {
        /// don't need to push an empty response to received_message_queue
        return GRPCReceiveQueueRes::OK;
    }

    GRPCReceiveQueueRes res = tryWriteImpl(received_message);

    if (likely(res == GRPCReceiveQueueRes::OK || res == GRPCReceiveQueueRes::FULL))
        ExchangeReceiverMetric::addDataSizeMetric(*data_size_in_queue, tracked_packet->getPacket().ByteSizeLong());
    LOG_TRACE(log, "push recv_msg to msg_channel, res:{}, enable_fine_grained_shuffle: {}, fine grained channel size: {}", magic_enum::enum_name(res), enable_fine_grained_shuffle, fine_grained_channel_size);
    return res;
}

GRPCReceiveQueueRes ReceiverChannelTryWriter::tryReWrite()
{
    GRPCReceiveQueueRes res = GRPCReceiveQueueRes::OK;
    if (rewrite_msg != nullptr)
    {
        res = tryRewriteImpl(rewrite_msg);
        if (res == GRPCReceiveQueueRes::OK)
        {
            rewrite_msg = nullptr;
        }
        /// if rewrite fails, wait for the next rewrite
    }

    return res;
}

GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWriteImpl(ReceivedMessagePtr & msg)
{
    assert(rewrite_msg == nullptr);

    GRPCReceiveQueueRes res = received_message_queue->pushToGRPCReceiveQueue(msg);

    if (res == GRPCReceiveQueueRes::FULL)
        rewrite_msg = std::move(msg);
    return res;
}

GRPCReceiveQueueRes ReceiverChannelTryWriter::tryRewriteImpl(ReceivedMessagePtr & msg)
{
    return received_message_queue->pushToGRPCReceiveQueue(msg);
}

} // namespace DB
