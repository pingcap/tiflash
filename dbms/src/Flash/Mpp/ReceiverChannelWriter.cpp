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

#include <Flash/Mpp/ReceiverChannelWriter.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <utility>

namespace DB
{
namespace
{
inline void injectFailPointReceiverPushFail(bool & push_succeed [[maybe_unused]], ReceiverMode mode)
{
    switch (mode)
    {
    case ReceiverMode::Local:
        fiu_do_on(FailPoints::random_receiver_local_msg_push_failure_failpoint, push_succeed = false);
        break;
    case ReceiverMode::Sync:
        fiu_do_on(FailPoints::random_receiver_sync_msg_push_failure_failpoint, push_succeed = false);
        break;
    default:
        throw Exception(fmt::format("Invalid ReceiverMode: {}", magic_enum::enum_name(mode)));
    }
}

bool loopJudge(GRPCReceiveQueueRes res) { return (res == GRPCReceiveQueueRes::OK || res == GRPCReceiveQueueRes::FULL); }

// result can not be changed from FULL to OK
void updateResult(GRPCReceiveQueueRes & dst, GRPCReceiveQueueRes & src)
{
    if (likely(!(dst == GRPCReceiveQueueRes::FULL && src == GRPCReceiveQueueRes::OK)))
        dst = src;
}
} // namespace

template <bool enable_fine_grained_shuffle>
bool ReceiverChannelWriter::write(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
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

template <bool enable_fine_grained_shuffle>
GRPCReceiveQueueRes ReceiverChannelWriter::tryWrite(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
{
    const mpp::Error * error_ptr = getErrorPtr(tracked_packet->packet);
    const String * resp_ptr = getRespPtr(tracked_packet->packet);

    GRPCReceiveQueueRes res;
    if constexpr (enable_fine_grained_shuffle)
        res = tryWriteFineGrain(source_index, tracked_packet, error_ptr, resp_ptr);
    else
        res = tryWriteNonFineGrain(source_index, tracked_packet, error_ptr, resp_ptr);

    if (res == GRPCReceiveQueueRes::FULL)
        LOG_INFO(log, "Profiling: channelreceiver tryWrite full...");

    if (likely(res == GRPCReceiveQueueRes::OK || res == GRPCReceiveQueueRes::FULL))
        ExchangeReceiverMetric::addDataSizeMetric(*data_size_in_queue, tracked_packet->getPacket().ByteSizeLong());
    LOG_TRACE(log, "push recv_msg to msg_channels(size: {}) res:{}, enable_fine_grained_shuffle: {}", msg_channels->size(), magic_enum::enum_name(res), enable_fine_grained_shuffle);
    return res;
}

bool ReceiverChannelWriter::splitPacketIntoChunks(size_t source_index, mpp::MPPDataPacket & packet, std::vector<std::vector<const String *>> & chunks)
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

    for (int i = 0; i < packet.stream_ids_size(); ++i)
    {
        UInt64 stream_id = packet.stream_ids(i) % msg_channels->size();
        chunks[stream_id].push_back(&packet.chunks(i));
    }

    return true;
}

bool ReceiverChannelWriter::writeFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr)
{
    bool success = true;
    auto & packet = tracked_packet->packet;
    std::vector<std::vector<const String *>> chunks(msg_channels->size());

    if (!splitPacketIntoChunks(source_index, packet, chunks))
        return false;

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

bool ReceiverChannelWriter::writeNonFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr)
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

GRPCReceiveQueueRes ReceiverChannelWriter::tryWriteFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr)
{
    GRPCReceiveQueueRes res = GRPCReceiveQueueRes::OK;
    auto & packet = tracked_packet->packet;
    std::vector<std::vector<const String *>> chunks(msg_channels->size());

    if (!splitPacketIntoChunks(source_index, packet, chunks))
        return GRPCReceiveQueueRes::CANCELLED;

    // Still need to send error_ptr or resp_ptr even if packet.chunks_size() is zero.
    for (size_t i = 0; i < msg_channels->size() && loopJudge(res); ++i)
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

GRPCReceiveQueueRes ReceiverChannelWriter::tryWriteNonFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr)
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
GRPCReceiveQueueRes ReceiverChannelWriter::tryReWrite()
{
    LOG_INFO(log, "Profiling: receiverchannel enter tryReWrite...");
    GRPCReceiveQueueRes res = GRPCReceiveQueueRes::OK;
    auto iter = rewrite_msgs.begin();

    while (loopJudge(res) && (iter != rewrite_msgs.end()))
    {
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

    LOG_INFO(log, "Profiling: receiverchannel tryReWrite {}...", magic_enum::enum_name(res));

    return res;
}

GRPCReceiveQueueRes ReceiverChannelWriter::tryWriteImpl(size_t index, std::shared_ptr<ReceivedMessage> && msg)
{
    GRPCReceiveQueueRes res = grpc_recv_queues[index]->push(std::move(msg), tag);
    if (res == GRPCReceiveQueueRes::FULL)
        rewrite_msgs.insert({index, std::move(msg)});
    return res;
}

GRPCReceiveQueueRes ReceiverChannelWriter::tryRewriteImpl(size_t index, std::shared_ptr<ReceivedMessage> & msg)
{
    GRPCReceiveQueueRes res = grpc_recv_queues[index]->push(msg, tag);
    return res;
}

template GRPCReceiveQueueRes ReceiverChannelWriter::tryReWrite<true>();
template GRPCReceiveQueueRes ReceiverChannelWriter::tryReWrite<false>();
template GRPCReceiveQueueRes ReceiverChannelWriter::tryWrite<true>(size_t, const TrackedMppDataPacketPtr &);
template GRPCReceiveQueueRes ReceiverChannelWriter::tryWrite<false>(size_t, const TrackedMppDataPacketPtr &);
template bool ReceiverChannelWriter::write<true>(size_t, const TrackedMppDataPacketPtr &);
template bool ReceiverChannelWriter::write<false>(size_t, const TrackedMppDataPacketPtr &);
} // namespace DB
