// Copyright 2022 PingCAP, Ltd.
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

#include <Common/ConcurrentIOQueue.h>
#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>

#include <functional>

namespace DB
{
namespace FailPoints
{
extern const char random_receiver_local_msg_push_failure_failpoint[];
extern const char random_receiver_sync_msg_push_failure_failpoint[];
extern const char random_receiver_async_msg_push_failure_failpoint[];
} // namespace FailPoints

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

    void switchMemTracker()
    {
        packet->switchMemTracker(current_memory_tracker);
    }
};
using ReceivedMessagePtr = std::shared_ptr<ReceivedMessage>;

enum class ReceiverMode
{
    Local = 0,
    Sync,
    Async
};

using MsgChannelPtr = std::shared_ptr<ConcurrentIOQueue<std::shared_ptr<ReceivedMessage>>>;

class ReceiverChannelWriter
{
public:
    ReceiverChannelWriter(std::vector<MsgChannelPtr> * msg_channels_, const String & req_info_, const LoggerPtr & log_, std::atomic<Int64> * data_size_in_queue_, ReceiverMode mode_)
        : data_size_in_queue(data_size_in_queue_)
        , msg_channels(msg_channels_)
        , req_info(req_info_)
        , log(log_)
        , mode(mode_)
    {}

    // "write" means writing the packet to the channel which is a ConcurrentIOQueue.
    //
    // If non_blocking:
    //    call ConcurrentIOQueue::nonBlockingPush
    // If !non_blocking:
    //    call ConcurrentIOQueue::push
    //
    // If enable_fine_grained_shuffle:
    //      Seperate chunks according to packet.stream_ids[i], then push to msg_channels[stream_id].
    // If fine grained_shuffle is disabled:
    //      Push all chunks to msg_channels[0].
    //
    // Return true if all push succeed, otherwise return false.
    // NOTE: shared_ptr<MPPDataPacket> will be hold by all ExchangeReceiverBlockInputStream to make chunk pointer valid.
    template <bool enable_fine_grained_shuffle, bool non_blocking = false>
    bool write(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
    {
        const auto & packet = tracked_packet->packet;
        const mpp::Error * error_ptr = packet.has_error() ? &packet.error() : nullptr;
        const String * resp_ptr = packet.data().empty() ? nullptr : &packet.data();

        WriteToChannelFunc write_func;
        if constexpr (non_blocking)
            write_func = [&](size_t i, ReceivedMessagePtr && recv_msg) {
                return (*msg_channels)[i]->nonBlockingPush(std::move(recv_msg));
            };
        else
            write_func = [&](size_t i, ReceivedMessagePtr && recv_msg) {
                return (*msg_channels)[i]->push(std::move(recv_msg));
            };

        bool success;
        if constexpr (enable_fine_grained_shuffle)
            success = writeFineGrain(write_func, source_index, tracked_packet, error_ptr, resp_ptr);
        else
            success = writeNonFineGrain(write_func, source_index, tracked_packet, error_ptr, resp_ptr);

        if (likely(success))
            ExchangeReceiverMetric::addDataSizeMetric(*data_size_in_queue, tracked_packet->getPacket().ByteSizeLong());
        LOG_TRACE(log, "push recv_msg to msg_channels(size: {}) succeed:{}, enable_fine_grained_shuffle: {}", msg_channels->size(), success, enable_fine_grained_shuffle);
        return success;
    }

    bool isReadyForWrite() const;

private:
    using WriteToChannelFunc = std::function<MPMCQueueResult(size_t, ReceivedMessagePtr &&)>;

    bool writeFineGrain(
        WriteToChannelFunc write_func,
        size_t source_index,
        const TrackedMppDataPacketPtr & tracked_packet,
        const mpp::Error * error_ptr,
        const String * resp_ptr);
    bool writeNonFineGrain(
        WriteToChannelFunc write_func,
        size_t source_index,
        const TrackedMppDataPacketPtr & tracked_packet,
        const mpp::Error * error_ptr,
        const String * resp_ptr);

    std::atomic<Int64> * data_size_in_queue;
    std::vector<MsgChannelPtr> * msg_channels;
    String req_info;
    const LoggerPtr log;
    ReceiverMode mode;
};
} // namespace DB
