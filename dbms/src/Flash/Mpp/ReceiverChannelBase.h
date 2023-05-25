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
#include <Common/TiFlashMetrics.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>

#include <memory>

namespace DB
{
namespace FailPoints
{
extern const char random_receiver_local_msg_push_failure_failpoint[];
extern const char random_receiver_sync_msg_push_failure_failpoint[];
extern const char random_receiver_async_msg_push_failure_failpoint[];
} // namespace FailPoints

enum class ReceiverMode
{
    Local = 0,
    Sync,
    Async
};

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

struct ReceivedMessage
{
    size_t source_index;
    String req_info;
    // shared_ptr<const MPPDataPacket> is copied to make sure error_ptr, resp_ptr and chunks are valid.
    const std::shared_ptr<DB::TrackedMppDataPacket> packet;
    const mpp::Error * error_ptr;
    const String * resp_ptr;
    std::vector<const String *> chunks;
    /// used for fine grained shuffle
    std::shared_ptr<std::atomic<size_t>> remaining_consumer;

    // Constructor that move chunks.
    ReceivedMessage(size_t source_index_,
                    const String & req_info_,
                    const std::shared_ptr<DB::TrackedMppDataPacket> & packet_,
                    const mpp::Error * error_ptr_,
                    const String * resp_ptr_,
                    std::vector<const String *> && chunks_)
        : source_index(source_index_)
        , req_info(req_info_)
        , packet(packet_)
        , error_ptr(error_ptr_)
        , resp_ptr(resp_ptr_)
        , chunks(chunks_)
    {}

    bool containUsefulMessage() const
    {
        return error_ptr != nullptr || resp_ptr != nullptr || !chunks.empty();
    }
};

using ReceivedMessagePtr = std::shared_ptr<ReceivedMessage>;
using MsgChannelPtr = std::shared_ptr<LooseBoundedMPMCQueue<std::shared_ptr<ReceivedMessage>>>;

struct ReceivedMessageQueue
{
    /// msg_channel is a bounded queue that saves the received messages
    /// msg_channels_for_fine_grained_shuffle is multiple unbounded queues that saves fine grained received messages
    /// all the received messages in msg_channels_for_fine_grained_shuffle must be saved in msg_channel first, so the
    /// total size/memory of `ReceivedMessageQueue` is still under control even if msg_channels_for_fine_grained_shuffle
    /// is unbounded queues
    /// for non fine grained shuffle, all the read/read to the queue is based on msg_channel/grpc_recv_queue
    /// for fine grained shuffle
    /// write: the channel writer first write the msg to msg_channel/grpc_recv_queue, if write success, then write msg to msg_channels_for_fine_grained_shuffle
    /// read: the reader read msg from msg_channels_for_fine_grained_shuffle, and reduce the `remaining_consumer` in msg, if `remaining_consumer` is 0, then
    ///       remove the msg from msg_channel/grpc_recv_queue
    MsgChannelPtr msg_channel;
    std::shared_ptr<GRPCReceiveQueue<ReceivedMessagePtr>> grpc_recv_queue;
    std::vector<MsgChannelPtr> msg_channels_for_fine_grained_shuffle;
    template <bool need_wait, bool fine_grained_shuffle>
    MPMCQueueResult pop(ReceivedMessagePtr recv_msg, size_t stream_id)
    {
        MPMCQueueResult res;
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
                    auto pop_result = grpc_recv_queue->tryPop(original_msg);
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
        return res;
    }
    void finish()
    {
        grpc_recv_queue->finish();
        for (auto & channel : msg_channels_for_fine_grained_shuffle)
            channel->finish();
    }
    void cancel()
    {
        grpc_recv_queue->cancel();
        for (auto & channel : msg_channels_for_fine_grained_shuffle)
            channel->cancel();
    }
};

ReceivedMessagePtr toReceivedMessage(
    const TrackedMppDataPacketPtr & tracked_packet,
    size_t source_index,
    const String & req_info,
    bool for_fine_grained_shuffle,
    size_t fine_grained_consumer_size);

void injectFailPointReceiverPushFail(bool & push_succeed [[maybe_unused]], ReceiverMode mode);

class ReceiverChannelBase
{
public:
    ReceiverChannelBase(ReceivedMessageQueue * received_message_queue_, const String & req_info_, const LoggerPtr & log_, std::atomic<Int64> * data_size_in_queue_, ReceiverMode mode_)
        : data_size_in_queue(data_size_in_queue_)
        , received_message_queue(received_message_queue_)
        , fine_grained_channel_size(received_message_queue->msg_channels_for_fine_grained_shuffle.size())
        , req_info(req_info_)
        , log(log_)
        , mode(mode_)
    {}

    ~ReceiverChannelBase() = default;

protected:
    bool splitFineGrainedShufflePacketIntoChunks(size_t source_index, mpp::MPPDataPacket & packet, std::vector<std::vector<const String *>> & chunks);
    bool writeMessageToFineGrainChannels(ReceivedMessagePtr original_message);

    std::atomic<Int64> * data_size_in_queue;
    ReceivedMessageQueue * received_message_queue = nullptr;
    size_t fine_grained_channel_size;
    String req_info;
    const LoggerPtr log;
    ReceiverMode mode;
};
} // namespace DB
