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

#include <Common/TiFlashMetrics.h>
#include <Flash/Mpp/ReceivedMessageQueue.h>

#include "Flash/Pipeline/Schedule/Tasks/NotifyFuture.h"

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
void injectFailPointReceiverPushFail(bool & push_succeed [[maybe_unused]], ReceiverMode mode [[maybe_unused]])
{
#ifndef NDEBUG
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
#endif
}

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

ReceivedMessagePtr toReceivedMessage(
    size_t source_index,
    const String & req_info,
    const TrackedMppDataPacketPtr & tracked_packet,
    size_t fine_grained_consumer_size)
{
    const auto & packet = tracked_packet->packet;
    const mpp::Error * error_ptr = getErrorPtr(packet);
    const String * resp_ptr = getRespPtr(packet);
    std::vector<const String *> chunks(packet.chunks_size());
    for (int i = 0; i < packet.chunks_size(); ++i)
        chunks[i] = &packet.chunks(i);
    return std::make_shared<ReceivedMessage>(
        source_index,
        req_info,
        tracked_packet,
        error_ptr,
        resp_ptr,
        std::move(chunks),
        fine_grained_consumer_size);
}
} // namespace

ReceivedMessageQueue::ReceivedMessageQueue(
    const CapacityLimits & queue_limits,
    const LoggerPtr & log_,
    std::atomic<Int64> * data_size_in_queue_,
    bool enable_fine_grained,
    size_t fine_grained_channel_size_)
    : log(log_)
    , data_size_in_queue(data_size_in_queue_)
    , fine_grained_channel_size(enable_fine_grained ? fine_grained_channel_size_ : 0)
    , grpc_recv_queue(
          log_,
          queue_limits,
          [](const ReceivedMessagePtr & message) { return message->getPacket().ByteSizeLong(); },
          /// use pushcallback to make sure that the order of messages in msg_channels_for_fine_grained_shuffle is exactly the same as it in msg_channel,
          /// because pop from msg_channel rely on this assumption. An alternative is to make msg_channel a set/map of messages for fine grained shuffle, but
          /// it need many more changes
          !enable_fine_grained
              ? nullptr
              : std::function<void(const ReceivedMessagePtr &)>([this](const ReceivedMessagePtr & element) {
                    for (size_t i = 0; i < fine_grained_channel_size; ++i)
                    {
                        auto result = msg_channels_for_fine_grained_shuffle[i].forcePush(element);
                        RUNTIME_CHECK_MSG(result == MPMCQueueResult::OK, "push to fine grained channel must success");
                    }
                }))
{
    if (enable_fine_grained)
    {
        assert(fine_grained_channel_size > 0);
        msg_channels_for_fine_grained_shuffle.reserve(fine_grained_channel_size);
        for (size_t i = 0; i < fine_grained_channel_size; ++i)
            msg_channels_for_fine_grained_shuffle.emplace_back();
    }
}

template <bool need_wait>
MPMCQueueResult ReceivedMessageQueue::pop(size_t stream_id, ReceivedMessagePtr & recv_msg)
{
    MPMCQueueResult res;
    if (fine_grained_channel_size > 0)
    {
        if constexpr (need_wait)
            res = msg_channels_for_fine_grained_shuffle[stream_id].pop(recv_msg);
        else
            res = msg_channels_for_fine_grained_shuffle[stream_id].tryPop(recv_msg);

        if (res == MPMCQueueResult::OK)
        {
            if (recv_msg->getRemainingConsumers()->fetch_sub(1) == 1)
            {
#ifndef NDEBUG
                ReceivedMessagePtr original_msg;
                auto pop_result = grpc_recv_queue.tryPop(original_msg);
                /// if there is no remaining consumer, then pop it from original queue, the message must stay in the queue before the pop
                /// so even use tryPop, the result must not be empty
                RUNTIME_CHECK_MSG(
                    pop_result != MPMCQueueResult::EMPTY,
                    "The result of 'grpc_recv_queue->tryPop' is definitely not EMPTY.");
                if likely (original_msg != nullptr)
                    RUNTIME_CHECK_MSG(
                        *original_msg->getRemainingConsumers() == 0,
                        "Fine grained receiver pop a message that is not full consumed, remaining consumer: {}",
                        *original_msg->getRemainingConsumers());
#else
                grpc_recv_queue.tryDequeue();
#endif
            }
            ExchangeReceiverMetric::subDataSizeMetric(*data_size_in_queue, recv_msg->getPacket().ByteSizeLong());
        }
        else
        {
            if constexpr (!need_wait)
            {
                if (res == MPMCQueueResult::EMPTY)
                    setNotifyFuture(&msg_channels_for_fine_grained_shuffle[stream_id]);
            }
        }
    }
    else
    {
        if constexpr (need_wait)
            res = grpc_recv_queue.pop(recv_msg);
        else
            res = grpc_recv_queue.tryPop(recv_msg);

        if (res == MPMCQueueResult::OK)
        {
            ExchangeReceiverMetric::subDataSizeMetric(*data_size_in_queue, recv_msg->getPacket().ByteSizeLong());
        }
        else
        {
            if constexpr (!need_wait)
            {
                if (res == MPMCQueueResult::EMPTY)
                    setNotifyFuture(&grpc_recv_queue);
            }
        }
    }
    return res;
}

template <bool is_force>
bool ReceivedMessageQueue::pushPacket(
    size_t source_index,
    const String & req_info,
    const TrackedMppDataPacketPtr & tracked_packet,
    ReceiverMode mode)
{
    auto received_message = toReceivedMessage(source_index, req_info, tracked_packet, fine_grained_channel_size);
    if (!received_message->containUsefulMessage())
        return true;

    bool success = false;
    if constexpr (is_force)
        success = grpc_recv_queue.forcePush(std::move(received_message)) == MPMCQueueResult::OK;
    else
        success = grpc_recv_queue.push(std::move(received_message)) == MPMCQueueResult::OK;

    if (success)
        ExchangeReceiverMetric::addDataSizeMetric(*data_size_in_queue, tracked_packet->getPacket().ByteSizeLong());

    injectFailPointReceiverPushFail(success, mode);
    return success;
}

MPMCQueueResult ReceivedMessageQueue::pushAsyncGRPCPacket(
    size_t source_index,
    const String & req_info,
    const TrackedMppDataPacketPtr & tracked_packet,
    GRPCKickTag * new_tag)
{
    auto received_message = toReceivedMessage(source_index, req_info, tracked_packet, fine_grained_channel_size);
    if (!received_message->containUsefulMessage())
        return MPMCQueueResult::OK;

    fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, return MPMCQueueResult::CANCELLED);

    auto res = grpc_recv_queue.pushWithTag(std::move(received_message), new_tag);
    if likely (res == MPMCQueueResult::OK || res == MPMCQueueResult::FULL)
        ExchangeReceiverMetric::addDataSizeMetric(*data_size_in_queue, tracked_packet->getPacket().ByteSizeLong());

    return res;
}

template MPMCQueueResult ReceivedMessageQueue::pop<true>(size_t stream_id, ReceivedMessagePtr & recv_msg);
template MPMCQueueResult ReceivedMessageQueue::pop<false>(size_t stream_id, ReceivedMessagePtr & recv_msg);
template bool ReceivedMessageQueue::pushPacket<true>(
    size_t source_index,
    const String & req_info,
    const TrackedMppDataPacketPtr & tracked_packet,
    ReceiverMode mode);
template bool ReceivedMessageQueue::pushPacket<false>(
    size_t source_index,
    const String & req_info,
    const TrackedMppDataPacketPtr & tracked_packet,
    ReceiverMode mode);

} // namespace DB
