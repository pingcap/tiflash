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

#include <Flash/Mpp/ReceiverChannelWriter.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <utility>

namespace DB
{
template <bool enable_fine_grained_shuffle>
bool ReceiverChannelWriter::write(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
{
    const mpp::Error * error_ptr = getErrorPtr(tracked_packet->packet);
    const String * resp_ptr = getRespPtr(tracked_packet->packet);

    bool success;
    if constexpr (enable_fine_grained_shuffle)
        success = writeFineGrain(source_index, tracked_packet, error_ptr, resp_ptr);
    else
        success = writeNonFineGrain(source_index, tracked_packet, error_ptr, resp_ptr);

    if (likely(success))
        ExchangeReceiverMetric::addDataSizeMetric(*data_size_in_queue, tracked_packet->getPacket().ByteSizeLong());
    LOG_TRACE(log, "push recv_msg to msg_channels(size: {}) succeed:{}, enable_fine_grained_shuffle: {}", msg_channels->size(), success, enable_fine_grained_shuffle);
    return success;
}

bool ReceiverChannelWriter::writeFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr)
{
    bool success = true;
    auto & packet = tracked_packet->packet;
    std::vector<std::vector<const String *>> chunks(msg_channels->size());

    if (!splitPacketIntoChunks(source_index, packet, chunks))
        return false;

    // Still need to send error_ptr or resp_ptr even if packet.chunks_size() is zero.
    for (size_t i = 0; i < msg_channels->size() && success; ++i)
    {
        if (resp_ptr == nullptr && error_ptr == nullptr && chunks[i].empty())
            continue;

        std::shared_ptr<ReceivedMessage> recv_msg = std::make_shared<ReceivedMessage>(
            source_index,
            req_info,
            tracked_packet,
            error_ptr,
            resp_ptr,
            std::move(chunks[i]));

        success = (*msg_channels)[i]->push(std::move(recv_msg)) == MPMCQueueResult::OK;
        injectFailPointReceiverPushFail(success, mode);

        // Only the first ExchangeReceiverInputStream need to handle resp.
        resp_ptr = nullptr;
    }
    return success;
}

bool ReceiverChannelWriter::writeNonFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr)
{
    bool success = true;
    auto & packet = tracked_packet->packet;
    std::vector<const String *> chunks(packet.chunks_size());

    for (int i = 0; i < packet.chunks_size(); ++i)
        chunks[i] = &packet.chunks(i);

    if (!(resp_ptr == nullptr && error_ptr == nullptr && chunks.empty()))
    {
        std::shared_ptr<ReceivedMessage> recv_msg = std::make_shared<ReceivedMessage>(
            source_index,
            req_info,
            tracked_packet,
            error_ptr,
            resp_ptr,
            std::move(chunks));

        success = (*msg_channels)[0]->push(std::move(recv_msg)) == MPMCQueueResult::OK;
        injectFailPointReceiverPushFail(success, mode);
    }
    return success;
}

template bool ReceiverChannelWriter::write<true>(size_t, const TrackedMppDataPacketPtr &);
template bool ReceiverChannelWriter::write<false>(size_t, const TrackedMppDataPacketPtr &);
} // namespace DB
