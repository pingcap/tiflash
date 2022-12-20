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

#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/MPMCQueue.h>

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

enum class ExchangeMode
{
    Local = 0,
    Sync,
    Async
};

using MsgChannelPtr = std::shared_ptr<MPMCQueue<std::shared_ptr<ReceivedMessage>>>;

inline void injectFailPointReceiverPushFail(bool & push_succeed, ExchangeMode mode)
{
    switch (mode) {
    case ExchangeMode::Local:
        fiu_do_on(FailPoints::random_receiver_local_msg_push_failure_failpoint, push_succeed = false);
        break;
    case ExchangeMode::Sync:
        fiu_do_on(FailPoints::random_receiver_sync_msg_push_failure_failpoint, push_succeed = false);
        break;
    case ExchangeMode::Async:
        fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, push_succeed = false);
        break;
    default:
        throw Exception("Unsupported ExchangeMode");
    }
}

class ReceiverChannelWriter
{
public:
    ReceiverChannelWriter(std::vector<MsgChannelPtr> * msg_channels_, const String & req_info_, const LoggerPtr & log_, std::atomic<Int64> * data_size_in_queue_, ExchangeMode mode_)
        : data_size_in_queue(data_size_in_queue_)
        , msg_channels(msg_channels_)
        , req_info(req_info_)
        , log(log_)
        , mode(mode_)
    {}

    // "write" means writing the packet to the channel which is a MPMCQueue.
    //
    // If enable_fine_grained_shuffle:
    //      Seperate chunks according to packet.stream_ids[i], then push to msg_channels[stream_id].
    // If fine grained_shuffle is disabled:
    //      Push all chunks to msg_channels[0].
    // Return true if all push succeed, otherwise return false.
    // NOTE: shared_ptr<MPPDataPacket> will be hold by all ExchangeReceiverBlockInputStream to make chunk pointer valid.
    template <bool enable_fine_grained_shuffle>
    bool write(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
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

    void setReqInfo(const String & req_info_) { req_info = req_info_; }

private:
    static const mpp::Error * getErrorPtr(const mpp::MPPDataPacket & packet)
    {
        if (unlikely(packet.has_error()))
            return &packet.error();
        return nullptr;
    }

    static const String * getRespPtr(const mpp::MPPDataPacket & packet)
    {
        if (unlikely(!packet.data().empty()))
            return &packet.data();
        return nullptr;
    }

    bool writeFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr)
    {
        bool success = true;
        auto & packet = tracked_packet->packet;
        std::vector<std::vector<const String *>> chunks(msg_channels->size());
        if (!packet.chunks().empty())
        {
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
            assert(packet.chunks_size() == packet.stream_ids_size());

            for (int i = 0; i < packet.stream_ids_size(); ++i)
            {
                UInt64 stream_id = packet.stream_ids(i) % msg_channels->size();
                chunks[stream_id].push_back(&packet.chunks(i));
            }
        }

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

    bool writeNonFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr)
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

    std::atomic<Int64> * data_size_in_queue;
    std::vector<MsgChannelPtr> * msg_channels;
    String req_info;
    const LoggerPtr log;
    ExchangeMode mode;
};
} // namespace DB
