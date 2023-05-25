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
#include <Flash/Mpp/ReceiverChannelBase.h>

#include <utility>

namespace DB
{
namespace
{
const mpp::Error * getErrorPtr(const mpp::MPPDataPacket & packet)
{
    if (unlikely(packet.has_error()))
        return &packet.error();
    return nullptr;
}

const String * getRespPtr(const mpp::MPPDataPacket & packet)
{
    if (unlikely(!packet.data().empty()))
        return &packet.data();
    return nullptr;
}
} // namespace
void injectFailPointReceiverPushFail(bool & push_succeed [[maybe_unused]], ReceiverMode mode)
{
    switch (mode)
    {
    case ReceiverMode::Local:
        fiu_do_on(FailPoints::random_receiver_local_msg_push_failure_failpoint, push_succeed = false);
        break;
    case ReceiverMode::Sync:
        fiu_do_on(FailPoints::random_receiver_sync_msg_push_failure_failpoint, push_succeed = false);
        break;
    case ReceiverMode::Async:
        fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, push_succeed = false);
        break;
    default:
        RUNTIME_ASSERT(false, "Illegal ReceiverMode");
    }
}

bool ReceiverChannelBase::splitFineGrainedShufflePacketIntoChunks(size_t source_index, mpp::MPPDataPacket & packet, std::vector<std::vector<const String *>> & chunks)
{
    if (packet.chunks().empty())
        return true;

    // Packet not empty.
    if (unlikely(packet.stream_ids().empty()))
    {
        // Fine grained shuffle is enabled in receiver, but sender didn't. We cannot handle this, so return error.
        // This can happen when there are old version nodes when upgrading.
        LOG_ERROR(log, "MPPDataPacket.stream_ids empty, it means ExchangeSender is old version of binary "
                       "(source_index: {}) while fine grained shuffle of ExchangeReceiver is enabled. "
                       "Cannot handle this.",
                  source_index);
        return false;
    }

    // packet.stream_ids[i] is corresponding to packet.chunks[i],
    // indicating which stream_id this chunk belongs to.
    RUNTIME_ASSERT(packet.chunks_size() == packet.stream_ids_size(), log, "packet's chunk size shoule be equal to it's size of streams");
    assert(fine_grained_channel_size > 0);

    for (int i = 0; i < packet.stream_ids_size(); ++i)
    {
        UInt64 stream_id = packet.stream_ids(i) % fine_grained_channel_size;
        chunks[stream_id].push_back(&packet.chunks(i));
    }

    return true;
}

bool ReceiverChannelBase::writeMessageToFineGrainChannels(ReceivedMessagePtr original_message)
{
    assert(fine_grained_channel_size > 0);
    bool success = true;
    auto & packet = original_message->packet->packet;
    std::vector<std::vector<const String *>> chunks(fine_grained_channel_size);
    if (!splitFineGrainedShufflePacketIntoChunks(original_message->source_index, packet, chunks))
        return false;
    const auto * resp_ptr = original_message->resp_ptr;
    const auto * error_ptr = original_message->error_ptr;
    for (size_t i = 0; i < fine_grained_channel_size && success; ++i)
    {
        auto recv_msg = std::make_shared<ReceivedMessage>(
            original_message->source_index,
            original_message->req_info,
            original_message->packet,
            error_ptr,
            resp_ptr,
            std::move(chunks[i]));
        recv_msg->remaining_consumer = original_message->remaining_consumer;
        auto push_result = received_message_queue->msg_channels_for_fine_grained_shuffle[i]->tryPush(std::move(recv_msg));
        /// the queue is unlimited, should never be full
        assert(push_result != MPMCQueueResult::FULL);
        success = push_result == MPMCQueueResult::OK;

        injectFailPointReceiverPushFail(success, mode);

        // Only the first ExchangeReceiverInputStream need to handle resp.
        resp_ptr = nullptr;
    }
    return success;
}

ReceivedMessagePtr toReceivedMessage(
    const TrackedMppDataPacketPtr & tracked_packet,
    size_t source_index,
    const String & req_info,
    bool for_fine_grained_shuffle,
    size_t fine_grained_consumer_size)
{
    const auto & packet = tracked_packet->packet;
    const mpp::Error * error_ptr = getErrorPtr(packet);
    const String * resp_ptr = getRespPtr(packet);
    std::vector<const String *> chunks(packet.chunks_size());
    for (int i = 0; i < packet.chunks_size(); ++i)
        chunks[i] = &packet.chunks(i);
    auto ret = std::make_shared<ReceivedMessage>(
        source_index,
        req_info,
        tracked_packet,
        error_ptr,
        resp_ptr,
        std::move(chunks));
    if (for_fine_grained_shuffle)
    {
        assert(fine_grained_consumer_size > 0);
        ret->remaining_consumer = std::make_shared<std::atomic<size_t>>(fine_grained_consumer_size);
    }
    return ret;
}
} // namespace DB
