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

#include <Common/Exception.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <Flash/Mpp/ReceiverChannelWriter.h>

namespace DB
{
template <bool enable_fine_grained_shuffle, bool is_force>
bool ReceiverChannelWriter::write(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
{
    auto received_message = toReceivedMessage(tracked_packet, source_index, req_info, enable_fine_grained_shuffle, fine_grained_channel_size);
    if (!received_message->containUsefulMessage())
    {
        /// don't need to push an empty response to received_message_queue
        return true;
    }

    std::function<MPMCQueueResult(ReceivedMessagePtr &)> write_func;
    if constexpr (is_force)
        write_func = [&](ReceivedMessagePtr & recv_msg) {
            return received_message_queue->msg_channel->forcePush(recv_msg);
        };
    else
        write_func = [&](ReceivedMessagePtr & recv_msg) {
            return received_message_queue->msg_channel->push(recv_msg);
        };

    bool success = write_func(received_message) == MPMCQueueResult::OK;
    injectFailPointReceiverPushFail(success, mode);
    if constexpr (enable_fine_grained_shuffle)
    {
        if (success)
            success = writeMessageToFineGrainChannels(received_message);
    }

    if (likely(success))
        ExchangeReceiverMetric::addDataSizeMetric(*data_size_in_queue, tracked_packet->getPacket().ByteSizeLong());
    LOG_TRACE(log, "push recv_msg to msg_channel succeed:{}, enable_fine_grained_shuffle: {}, fine grained channel size: {}", success, enable_fine_grained_shuffle, fine_grained_channel_size);
    return success;
}

bool ReceiverChannelWriter::isWritable() const
{
    return received_message_queue->msg_channel->isFull();
}

template bool ReceiverChannelWriter::write<true, true>(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet);
template bool ReceiverChannelWriter::write<true, false>(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet);
template bool ReceiverChannelWriter::write<false, true>(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet);
template bool ReceiverChannelWriter::write<false, false>(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet);

} // namespace DB
