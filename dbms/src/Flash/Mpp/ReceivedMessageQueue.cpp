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

ReceivedMessageQueue::ReceivedMessageQueue(
    const LoggerPtr & log_,
    const CapacityLimits & queue_limits,
    bool enable_fine_grained,
    size_t fine_grained_channel_size_)
    : log(log_)
    , fine_grained_channel_size(enable_fine_grained ? fine_grained_channel_size_ : 0)
    , grpc_recv_queue(
          log_,
          queue_limits,
          [](const ReceivedMessagePtr & message) { return message->getPacket().ByteSizeLong(); },
          /// use pushcallback to make sure that the order of messages in msg_channels_for_fine_grained_shuffle is exactly the same as it in msg_channel,
          /// because pop from msg_channel rely on this assumption. An alternative is to make msg_channel a set/map of messages for fine grained shuffle, but
          /// it need many more changes
          !enable_fine_grained ? nullptr : std::function<void(const ReceivedMessagePtr &)>([this](const ReceivedMessagePtr & element) {
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
            /// these are unbounded queues
            msg_channels_for_fine_grained_shuffle.emplace_back(std::numeric_limits<size_t>::max());

    }
}

template <bool need_wait>
MPMCQueueResult ReceivedMessageQueue::pop(ReceivedMessagePtr & message, size_t stream_id)
{
    MPMCQueueResult res;
    if (fine_grained_channel_size > 0)
    {
        if constexpr (need_wait)
            res = msg_channels_for_fine_grained_shuffle[stream_id].pop(message);
        else
            res = msg_channels_for_fine_grained_shuffle[stream_id].tryPop(message);

        if (message != nullptr)
        {
            if (message->getRemainingConsumers()->fetch_sub(1) == 1)
            {
                ReceivedMessagePtr original_msg;
                auto pop_result [[maybe_unused]] = grpc_recv_queue.tryPop(original_msg);
                /// if there is no remaining consumer, then pop it from original queue, the message must stay in the queue before the pop
                /// so even use tryPop, the result must not be empty
                assert(pop_result != MPMCQueueResult::EMPTY);
                if likely (original_msg != nullptr)
                    RUNTIME_CHECK_MSG(*original_msg->getRemainingConsumers() == 0, "Fine grained receiver pop a message that is not full consumed, remaining consumer: {}", *original_msg->getRemainingConsumers());
            }
        }
    }
    else
    {
        if constexpr (need_wait)
            res = grpc_recv_queue.pop(message);
        else
            res = grpc_recv_queue.tryPop(message);
    }
    return res;
}

template <bool is_force>
bool ReceivedMessageQueue::pushFromLocal(ReceivedMessagePtr && received_message, ReceiverMode mode)
{
    bool success = false;
    if constexpr (is_force)
        success = grpc_recv_queue.forcePush(std::move(received_message)) == MPMCQueueResult::OK;
    else
        success = grpc_recv_queue.push(std::move(received_message)) == MPMCQueueResult::OK;

    injectFailPointReceiverPushFail(success, mode);
    return success;
}

MPMCQueueResult ReceivedMessageQueue::pushFromRemote(ReceivedMessagePtr && received_message, GRPCKickTag * new_tag)
{
    auto res = grpc_recv_queue.push(std::move(received_message), new_tag);
    fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, res = MPMCQueueResult::CANCELLED);
    return res;
}

template MPMCQueueResult ReceivedMessageQueue::pop<true>(ReceivedMessagePtr & message, size_t stream_id);
template MPMCQueueResult ReceivedMessageQueue::pop<false>(ReceivedMessagePtr & message, size_t stream_id);
template bool ReceivedMessageQueue::pushFromLocal<true>(ReceivedMessagePtr && received_message, ReceiverMode mode);
template bool ReceivedMessageQueue::pushFromLocal<false>(ReceivedMessagePtr && received_message, ReceiverMode mode);

} // namespace DB
