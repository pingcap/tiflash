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

#include <Flash/Mpp/ReceivedMessageQueue.h>

namespace DB
{
namespace FailPoints
{
extern const char random_receiver_local_msg_push_failure_failpoint[];
extern const char random_receiver_sync_msg_push_failure_failpoint[];
extern const char random_receiver_async_msg_push_failure_failpoint[];
} // namespace FailPoints

namespace
{
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
} // namespace

bool ReceivedMessageQueue::splitFineGrainedShufflePacketIntoChunks(size_t source_index, mpp::MPPDataPacket & packet, std::vector<std::vector<const String *>> & chunks)
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

bool ReceivedMessageQueue::writeMessageToFineGrainChannels(ReceivedMessagePtr original_message, ReceiverMode mode)
{
    assert(fine_grained_channel_size > 0);
    bool success = true;
    auto & packet = original_message->packet->packet;
    std::vector<std::vector<const String *>> chunks(msg_channels_for_fine_grained_shuffle.size());
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
        auto push_result = msg_channels_for_fine_grained_shuffle[i]->tryPush(std::move(recv_msg));
        /// the queue is unlimited, should never be full
        assert(push_result != MPMCQueueResult::FULL);
        success = push_result == MPMCQueueResult::OK;

        injectFailPointReceiverPushFail(success, mode);
        // Only the first ExchangeReceiverInputStream need to handle resp.
        resp_ptr = nullptr;
    }
    return success;
}

template <bool need_wait, bool fine_grained_shuffle>
std::pair<MPMCQueueResult, ReceivedMessagePtr> ReceivedMessageQueue::pop(size_t stream_id)
{
    MPMCQueueResult res;
    ReceivedMessagePtr recv_msg;
    if constexpr (fine_grained_shuffle)
    {
        if constexpr (need_wait)
        {
            res = msg_channels_for_fine_grained_shuffle[stream_id]->pop(recv_msg);
        }
        else
        {
            res = msg_channels_for_fine_grained_shuffle[stream_id]->tryPop(recv_msg);
        }
        if (recv_msg != nullptr)
        {
            if (recv_msg->remaining_consumer->fetch_sub(1) == 1)
            {
                /// if there is no consumer, then pop it from original queue
                ReceivedMessagePtr original_msg;
                auto pop_result [[maybe_unused]] = grpc_recv_queue->tryPop(original_msg);
                assert(pop_result != MPMCQueueResult::EMPTY);
                if (original_msg != nullptr)
                    assert(*original_msg->remaining_consumer == 0);
            }
        }
    }
    else
    {
        if constexpr (need_wait)
        {
            res = grpc_recv_queue->pop(recv_msg);
        }
        else
        {
            res = grpc_recv_queue->tryPop(recv_msg);
        }
    }
    return {res, recv_msg};
}

template <bool is_force, bool enable_fine_grained_shuffle>
bool ReceivedMessageQueue::pushToMessageChannel(ReceivedMessagePtr & received_message, ReceiverMode mode)
{
    std::function<MPMCQueueResult(ReceivedMessagePtr &)> write_func;
    if constexpr (is_force)
        write_func = [&](ReceivedMessagePtr & recv_msg) {
            return msg_channel->forcePush(recv_msg);
        };
    else
        write_func = [&](ReceivedMessagePtr & recv_msg) {
            return msg_channel->push(recv_msg);
        };
    bool success = write_func(received_message) == MPMCQueueResult::OK;
    if constexpr (enable_fine_grained_shuffle)
    {
        if (success)
            success = writeMessageToFineGrainChannels(received_message, mode);
    }

    injectFailPointReceiverPushFail(success, mode);
    return success;
}

template <bool enable_fine_grained_shuffle>
GRPCReceiveQueueRes ReceivedMessageQueue::pushToGRPCReceiveQueue(ReceivedMessagePtr & received_message)
{
    auto res = grpc_recv_queue->push(received_message);
    if constexpr (enable_fine_grained_shuffle)
    {
        if (res == GRPCReceiveQueueRes::OK)
        {
            /// if write to first queue success, then write the message to fine grain queues
            if (!writeMessageToFineGrainChannels(received_message, ReceiverMode::Async))
                res = GRPCReceiveQueueRes::CANCELLED;
        }
    }
    fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, res = GRPCReceiveQueueRes::CANCELLED);
    return res;
}

ReceivedMessageQueue::ReceivedMessageQueue(
    const AsyncRequestHandlerWaitQueuePtr & conn_wait_queue,
    const LoggerPtr & log_,
    size_t max_buffer_size,
    bool enable_fine_grained,
    size_t fine_grained_channel_size_)
    : fine_grained_channel_size(enable_fine_grained ? fine_grained_channel_size_ : 0)
    , log(log_)
{
    msg_channel = std::make_shared<LooseBoundedMPMCQueue<ReceivedMessagePtr>>(max_buffer_size);
    grpc_recv_queue = std::make_shared<GRPCReceiveQueue<ReceivedMessagePtr>>(msg_channel, conn_wait_queue, log_);
    if (enable_fine_grained)
    {
        for (size_t i = 0; i < fine_grained_channel_size; ++i)
            /// these are unbounded queues
            msg_channels_for_fine_grained_shuffle.push_back(std::make_shared<LooseBoundedMPMCQueue<ReceivedMessagePtr>>(std::numeric_limits<size_t>::max()));
    }
}

template std::pair<MPMCQueueResult, ReceivedMessagePtr> ReceivedMessageQueue::pop<true, true>(size_t stream_id);
template std::pair<MPMCQueueResult, ReceivedMessagePtr> ReceivedMessageQueue::pop<true, false>(size_t stream_id);
template std::pair<MPMCQueueResult, ReceivedMessagePtr> ReceivedMessageQueue::pop<false, true>(size_t stream_id);
template std::pair<MPMCQueueResult, ReceivedMessagePtr> ReceivedMessageQueue::pop<false, false>(size_t stream_id);
template bool ReceivedMessageQueue::pushToMessageChannel<true, true>(ReceivedMessagePtr & received_message, ReceiverMode mode);
template bool ReceivedMessageQueue::pushToMessageChannel<true, false>(ReceivedMessagePtr & received_message, ReceiverMode mode);
template bool ReceivedMessageQueue::pushToMessageChannel<false, true>(ReceivedMessagePtr & received_message, ReceiverMode mode);
template bool ReceivedMessageQueue::pushToMessageChannel<false, false>(ReceivedMessagePtr & received_message, ReceiverMode mode);
template GRPCReceiveQueueRes ReceivedMessageQueue::pushToGRPCReceiveQueue<true>(ReceivedMessagePtr & received_message);
template GRPCReceiveQueueRes ReceivedMessageQueue::pushToGRPCReceiveQueue<false>(ReceivedMessagePtr & received_message);

} // namespace DB
