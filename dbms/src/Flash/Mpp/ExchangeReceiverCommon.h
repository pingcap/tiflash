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

#include <Common/FailPoint.h>
#include <Common/MPMCQueue.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadManager.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DecodeDetail.h>
#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Storages/StorageDisaggregated.h>
#include <kvproto/mpp.pb.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#include <cerrno>

#include "common/defines.h"

namespace DB
{
namespace FailPoints
{
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

constexpr Int32 batch_packet_count = 16;

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

struct ExchangeReceiverResult
{
    std::shared_ptr<tipb::SelectResponse> resp;
    size_t call_index;
    String req_info;
    bool meet_error;
    String error_msg;
    bool eof;
    DecodeDetail decode_detail;

    ExchangeReceiverResult()
        : ExchangeReceiverResult(nullptr, 0)
    {}

    static ExchangeReceiverResult newOk(std::shared_ptr<tipb::SelectResponse> resp_, size_t call_index_, const String & req_info_)
    {
        return {resp_, call_index_, req_info_, /*meet_error*/ false, /*error_msg*/ "", /*eof*/ false};
    }

    static ExchangeReceiverResult newEOF(const String & req_info_)
    {
        return {/*resp*/ nullptr, 0, req_info_, /*meet_error*/ false, /*error_msg*/ "", /*eof*/ true};
    }

    static ExchangeReceiverResult newError(size_t call_index, const String & req_info, const String & error_msg)
    {
        return {/*resp*/ nullptr, call_index, req_info, /*meet_error*/ true, error_msg, /*eof*/ false};
    }

private:
    ExchangeReceiverResult(
        std::shared_ptr<tipb::SelectResponse> resp_,
        size_t call_index_,
        const String & req_info_ = "",
        bool meet_error_ = false,
        const String & error_msg_ = "",
        bool eof_ = false)
        : resp(resp_)
        , call_index(call_index_)
        , req_info(req_info_)
        , meet_error(meet_error_)
        , error_msg(error_msg_)
        , eof(eof_)
    {}
};

enum class ExchangeReceiverState
{
    NORMAL,
    ERROR,
    CANCELED,
    CLOSED,
};

using MsgChannelPtr = std::shared_ptr<MPMCQueue<std::shared_ptr<ReceivedMessage>>>;

template <bool is_sync>
inline void injectFailPointReceiverPushFail(bool & push_succeed)
{
    if constexpr (is_sync)
        fiu_do_on(FailPoints::random_receiver_sync_msg_push_failure_failpoint, push_succeed = false;);
    else
        fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, push_succeed = false;);
}

class ReceiverChannelWriter
{
public:
    ReceiverChannelWriter(std::vector<MsgChannelPtr> * msg_channels_, const String & req_info_, const LoggerPtr & log_, std::atomic<Int64> * data_size_in_queue_)
        : data_size_in_queue(data_size_in_queue_)
        , msg_channels(msg_channels_)
        , req_info(req_info_)
        , log(log_)
    {}

    // "write" means writing the packet to the channel which is a MPMCQueue.
    //
    // If enable_fine_grained_shuffle:
    //      Seperate chunks according to packet.stream_ids[i], then push to msg_channels[stream_id].
    // If fine grained_shuffle is disabled:
    //      Push all chunks to msg_channels[0].
    // Return true if all push succeed, otherwise return false.
    // NOTE: shared_ptr<MPPDataPacket> will be hold by all ExchangeReceiverBlockInputStream to make chunk pointer valid.
    template <bool enable_fine_grained_shuffle, bool is_sync>
    bool write(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
    {
        const mpp::Error * error_ptr = getErrorPtr(tracked_packet->packet);
        const String * resp_ptr = getRespPtr(tracked_packet->packet);

        bool success;
        if constexpr (enable_fine_grained_shuffle)
            success = writeFineGrain<is_sync>(source_index, tracked_packet, error_ptr, resp_ptr);
        else
            success = writeNonFineGrain<is_sync>(source_index, tracked_packet, error_ptr, resp_ptr);

        if (likely(success))
            ExchangeReceiverMetric::addDataSizeMetric(*data_size_in_queue, tracked_packet->getPacket().ByteSizeLong());
        LOG_TRACE(log, "push recv_msg to msg_channels(size: {}) succeed:{}, enable_fine_grained_shuffle: {}", msg_channels->size(), success, enable_fine_grained_shuffle);
        return success;
    }

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

    template <bool is_sync>
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

            injectFailPointReceiverPushFail<is_sync>(success);

            // Only the first ExchangeReceiverInputStream need to handle resp.
            resp_ptr = nullptr;
        }
        return success;
    }

    template <bool is_sync>
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
            injectFailPointReceiverPushFail<is_sync>(success);
        }
        return success;
    }

    std::atomic<Int64> * data_size_in_queue;
    std::vector<MsgChannelPtr> * msg_channels;
    String req_info;
    const LoggerPtr log;
};

class ExchangeReceiverBase
{
public:
    static constexpr bool is_streaming_reader = true;
    static constexpr auto name = "ExchangeReceiver";

public:
    ExchangeReceiverBase(
        size_t source_num_,
        size_t max_streams_,
        const String & req_id,
        const String & executor_id,
        uint64_t fine_grained_shuffle_stream_count_,
        const std::vector<StorageDisaggregated::RequestAndRegionIDs> & disaggregated_dispatch_reqs_ = {})
        : source_num(source_num_)
        , enable_fine_grained_shuffle_flag(enableFineGrainedShuffle(fine_grained_shuffle_stream_count_))
        , output_stream_count(enable_fine_grained_shuffle_flag ? std::min(max_streams_, fine_grained_shuffle_stream_count_) : max_streams_)
        , max_buffer_size(std::max<size_t>(batch_packet_count, std::max(source_num, max_streams_) * 2))
        , thread_manager(newThreadManager())
        , live_connections(source_num)
        , state(ExchangeReceiverState::NORMAL)
        , exc_log(Logger::get(req_id, executor_id))
        , local_conn_num(0)
        , collected(false)
        , memory_tracker(current_memory_tracker)
        , disaggregated_dispatch_reqs(disaggregated_dispatch_reqs_)
    {}

    ~ExchangeReceiverBase();

    void close();

    const DAGSchema & getOutputSchema() const { return schema; }

    ExchangeReceiverResult nextResult(
        std::queue<Block> & block_queue,
        const Block & header,
        size_t stream_id,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

    size_t getSourceNum() const { return source_num; }
    uint64_t getFineGrainedShuffleStreamCount() const { return enable_fine_grained_shuffle_flag ? output_stream_count : 0; }
    int getExternalThreadCnt() const { return thread_count; }
    std::vector<MsgChannelPtr> & getMsgChannels() { return msg_channels; }

    bool isFineGrainedShuffleEnabled() const { return enable_fine_grained_shuffle_flag; }

    void connectionLocalDone(
        bool meet_error,
        const String & local_err_msg,
        const LoggerPtr & log);

    MemoryTracker * getMemoryTracker() const { return memory_tracker; }

    std::atomic<Int64> * getDataSizeInQueue() { return &data_size_in_queue; }

protected:
    std::shared_ptr<MemoryTracker> mem_tracker;

    bool setEndState(ExchangeReceiverState new_state);
    String getStatusString();

    ExchangeReceiverResult handleUnnormalChannel(
        std::queue<Block> & block_queue,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

    static DecodeDetail decodeChunks(
        const std::shared_ptr<ReceivedMessage> & recv_msg,
        std::queue<Block> & block_queue,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

    void connectionDone(
        bool meet_error,
        const String & local_err_msg,
        const LoggerPtr & log);

    // Local tunnel sender holds the pointer of ExchangeReceiverBase, so we must
    // ensure that ExchangeReceiverBase is destructed after all tunnel senders.
    void waitAllLocalConnDone();

    void finishAllMsgChannels();
    void cancelAllMsgChannels();

    ExchangeReceiverResult toDecodeResult(
        std::queue<Block> & block_queue,
        const Block & header,
        const std::shared_ptr<ReceivedMessage> & recv_msg,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

protected:
    void prepareMsgChannels();

    bool isReceiverForTiFlashStorage()
    {
        // If not empty, need to send MPPTask to tiflash_storage.
        return !disaggregated_dispatch_reqs.empty();
    }

    const tipb::ExchangeReceiver pb_exchange_receiver;
    const size_t source_num;
    const ::mpp::TaskMeta task_meta;
    const bool enable_fine_grained_shuffle_flag;
    const size_t output_stream_count;
    const size_t max_buffer_size;

    std::shared_ptr<ThreadManager> thread_manager;
    DAGSchema schema;

    std::vector<MsgChannelPtr> msg_channels;

    std::mutex mu;
    /// should lock `mu` when visit these members
    Int32 live_connections;
    ExchangeReceiverState state;
    String err_msg;

    LoggerPtr exc_log;

    Int32 local_conn_num;
    std::condition_variable local_conn_cv;

    bool collected = false;
    int thread_count = 0;
    MemoryTracker * memory_tracker;

    std::atomic<Int64> data_size_in_queue{};

    // For tiflash_compute node, need to send MPPTask to tiflash_storage node.
    std::vector<StorageDisaggregated::RequestAndRegionIDs> disaggregated_dispatch_reqs;
};
} // namespace DB
