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

#include <Common/MPMCQueue.h>
#include <Common/FailPoint.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DecodeDetail.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <kvproto/mpp.pb.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

namespace DB
{
namespace FailPoints
{
extern const char random_receiver_sync_msg_push_failure_failpoint[];
extern const char random_receiver_async_msg_push_failure_failpoint[];
} // namespace FailPoints

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

// If enable_fine_grained_shuffle:
//      Seperate chunks according to packet.stream_ids[i], then push to msg_channels[stream_id].
// If fine grained_shuffle is disabled:
//      Push all chunks to msg_channels[0].
// Return true if all push succeed, otherwise return false.
// NOTE: shared_ptr<MPPDataPacket> will be hold by all ExchangeReceiverBlockInputStream to make chunk pointer valid.
template <bool enable_fine_grained_shuffle, bool is_sync>
bool pushPacketImpl(size_t source_index,
                    const String & req_info,
                    const TrackedMppDataPacketPtr & tracked_packet,
                    const std::vector<MsgChannelPtr> & msg_channels,
                    LoggerPtr & log)
{
    bool push_succeed = true;

    const mpp::Error * error_ptr = nullptr;
    auto & packet = tracked_packet->packet;
    if (packet.has_error())
        error_ptr = &packet.error();

    const String * resp_ptr = nullptr;
    if (!packet.data().empty())
        resp_ptr = &packet.data();

    if constexpr (enable_fine_grained_shuffle)
    {
        std::vector<std::vector<const String *>> chunks(msg_channels.size());
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
                UInt64 stream_id = packet.stream_ids(i) % msg_channels.size();
                chunks[stream_id].push_back(&packet.chunks(i));
            }
        }
        // Still need to send error_ptr or resp_ptr even if packet.chunks_size() is zero.
        for (size_t i = 0; i < msg_channels.size() && push_succeed; ++i)
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
            push_succeed = msg_channels[i]->push(std::move(recv_msg)) == MPMCQueueResult::OK;
            if constexpr (is_sync)
                fiu_do_on(FailPoints::random_receiver_sync_msg_push_failure_failpoint, push_succeed = false;);
            else
                fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, push_succeed = false;);

            // Only the first ExchangeReceiverInputStream need to handle resp.
            resp_ptr = nullptr;
        }
    }
    else
    {
        std::vector<const String *> chunks(packet.chunks_size());
        for (int i = 0; i < packet.chunks_size(); ++i)
        {
            chunks[i] = &packet.chunks(i);
        }

        if (!(resp_ptr == nullptr && error_ptr == nullptr && chunks.empty()))
        {
            std::shared_ptr<ReceivedMessage> recv_msg = std::make_shared<ReceivedMessage>(
                source_index,
                req_info,
                tracked_packet,
                error_ptr,
                resp_ptr,
                std::move(chunks));

            push_succeed = msg_channels[0]->push(std::move(recv_msg)) == MPMCQueueResult::OK;
            if constexpr (is_sync)
                fiu_do_on(FailPoints::random_receiver_sync_msg_push_failure_failpoint, push_succeed = false;);
            else
                fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, push_succeed = false;);
        }
    }
    LOG_TRACE(log, "push recv_msg to msg_channels(size: {}) succeed:{}, enable_fine_grained_shuffle: {}", msg_channels.size(), push_succeed, enable_fine_grained_shuffle);
    return push_succeed;
}
} // namespace DB
