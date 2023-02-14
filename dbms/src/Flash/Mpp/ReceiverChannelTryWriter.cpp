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
#include <Flash/Mpp/ReceiverChannelTryWriter.h>

#include <utility>

namespace DB
{
template <bool enable_fine_grained_shuffle>
GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWrite(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
{
    const mpp::Error * error_ptr = getErrorPtr(tracked_packet->packet);
    const String * resp_ptr = getRespPtr(tracked_packet->packet);

    GRPCReceiveQueueRes res;
    if constexpr (enable_fine_grained_shuffle)
        res = tryWriteFineGrain(source_index, tracked_packet, error_ptr, resp_ptr);
    else
        res = tryWriteNonFineGrain(source_index, tracked_packet, error_ptr, resp_ptr);

    // debug
    if (res == GRPCReceiveQueueRes::FULL)
        LOG_INFO(log, "Profiling: channelreceiver tryWrite full...");

    if (likely(res == GRPCReceiveQueueRes::OK || res == GRPCReceiveQueueRes::FULL))
        ExchangeReceiverMetric::addDataSizeMetric(*data_size_in_queue, tracked_packet->getPacket().ByteSizeLong());
    LOG_TRACE(log, "push recv_msg to msg_channels(size: {}) res:{}, enable_fine_grained_shuffle: {}", channel_size, magic_enum::enum_name(res), enable_fine_grained_shuffle);
    return res;
}

GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWriteFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr)
{
    GRPCReceiveQueueRes res = GRPCReceiveQueueRes::OK;
    auto & packet = tracked_packet->packet;
    std::vector<std::vector<const String *>> chunks(channel_size);

    if (!splitPacketIntoChunks(source_index, packet, chunks))
        return GRPCReceiveQueueRes::CANCELLED;

    // Still need to send error_ptr or resp_ptr even if packet.chunks_size() is zero.
    for (size_t i = 0; i < channel_size && loopJudge(res); ++i)
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

        GRPCReceiveQueueRes write_res = tryWriteImpl(i, std::move(recv_msg));

        updateResult(res, write_res);
        fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, res = GRPCReceiveQueueRes::CANCELLED);

        // Only the first ExchangeReceiverInputStream need to handle resp.
        resp_ptr = nullptr;
    }
    return res;
}

GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWriteNonFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr)
{
    GRPCReceiveQueueRes res = GRPCReceiveQueueRes::OK;
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

        res = tryWriteImpl(0, std::move(recv_msg));
        fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, res = GRPCReceiveQueueRes::CANCELLED);
    }
    return res;
}

template <bool enable_fine_grained_shuffle>
GRPCReceiveQueueRes ReceiverChannelTryWriter::tryReWrite()
{
    // debug
    LOG_INFO(log, "Profiling: receiverchannel enter tryReWrite...");
    GRPCReceiveQueueRes res = GRPCReceiveQueueRes::OK;
    auto iter = rewrite_msgs.begin();

    while (loopJudge(res) && (iter != rewrite_msgs.end()))
    {
        // debug
        LOG_INFO(log, "Profiling: receiverchannel enter tryReWrite loop...");
        GRPCReceiveQueueRes write_res = tryRewriteImpl(iter->first, iter->second);
        if (write_res == GRPCReceiveQueueRes::OK)
        {
            auto tmp_iter = iter;
            ++iter;
            rewrite_msgs.erase(tmp_iter);
        }
        else
            ++iter;

        updateResult(res, write_res);
        fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, res = GRPCReceiveQueueRes::CANCELLED);
    }

    // debug
    LOG_INFO(log, "Profiling: receiverchannel tryReWrite {}...", magic_enum::enum_name(res));

    return res;
}

GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWriteImpl(size_t index, std::shared_ptr<ReceivedMessage> && msg)
{
    GRPCReceiveQueueRes res = grpc_recv_queues[index].push(std::move(msg));
    if (res == GRPCReceiveQueueRes::FULL)
        rewrite_msgs.insert({index, std::move(msg)});
    return res;
}

GRPCReceiveQueueRes ReceiverChannelTryWriter::tryRewriteImpl(size_t index, std::shared_ptr<ReceivedMessage> & msg)
{
    return grpc_recv_queues[index].push(msg);
}

template GRPCReceiveQueueRes ReceiverChannelTryWriter::tryReWrite<true>();
template GRPCReceiveQueueRes ReceiverChannelTryWriter::tryReWrite<false>();
template GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWrite<true>(size_t, const TrackedMppDataPacketPtr &);
template GRPCReceiveQueueRes ReceiverChannelTryWriter::tryWrite<false>(size_t, const TrackedMppDataPacketPtr &);
} // namespace DB
