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
#include <Common/MPMCQueue.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>

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

enum class ReceiverMode
{
    Local = 0,
    Sync,
    Async
};

using MsgChannelPtr = std::shared_ptr<MPMCQueue<std::shared_ptr<ReceivedMessage>>>;

class ReceiverChannelWriter
{
public:
    ReceiverChannelWriter(std::vector<MsgChannelPtr> * msg_channels_, const String & req_info_, const LoggerPtr & log_, std::atomic<Int64> * data_size_in_queue_, ReceiverMode mode_)
        : data_size_in_queue(data_size_in_queue_)
        , msg_channels(msg_channels_)
        , req_info(req_info_)
        , log(log_)
        , mode(mode_)
        , tag(nullptr)
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
    bool write(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet);

    template <bool enable_fine_grained_shuffle>
    GRPCReceiveQueueRes tryWrite(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet);

    template <bool enable_fine_grained_shuffle>
    GRPCReceiveQueueRes tryReWrite();

private:
    bool splitPacketIntoChunks(size_t source_index, mpp::MPPDataPacket & packet, std::vector<std::vector<const String *>> & chunks);

    // We must call this function before calling tryWrite().
    template <typename AsyncReader>
    void enableTryWriteMode(const std::shared_ptr<AsyncReader> & reader, void * tag_)
    {
        tag = tag_;
        for (auto & channel_ptr : *msg_channels)
            grpc_recv_queues.emplace_back(channel_ptr, reader->client_context.c_call(), log);
    }

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

    bool writeFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr);
    bool writeNonFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr);

    GRPCReceiveQueueRes tryWriteFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr);
    GRPCReceiveQueueRes tryWriteNonFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr);

    GRPCReceiveQueueRes tryWrite(size_t index, std::shared_ptr<ReceivedMessage> && msg);

    std::atomic<Int64> * data_size_in_queue;
    std::vector<MsgChannelPtr> * msg_channels;
    String req_info;
    const LoggerPtr log;
    ReceiverMode mode;
    std::vector<GRPCReceiveQueue<ReceivedMessage>> grpc_recv_queues;

    // This tag is used for tryWrite
    void * tag;

    // Push data may fail, so we need to save the message and re-push it at the proper time.
    std::pair<size_t, std::shared_ptr<ReceivedMessage>> rewrite_msg;
};
} // namespace DB
