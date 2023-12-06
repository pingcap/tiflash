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

#include <Flash/Mpp/ReceivedMessage.h>

namespace DB
{
bool ReceivedMessage::hasData(size_t stream_id) const
{
    if (remaining_consumers != nullptr)
    {
        if (is_local)
        {
            assert(fine_grained_blocks.size() > stream_id);
            return !fine_grained_blocks[stream_id].empty();
        }
        else
        {
            assert(fine_grained_chunks.size() > stream_id);
            return !fine_grained_chunks[stream_id].empty();
        }
    }
    else
    {
        return is_local ? !blocks.empty() : !chunks.empty();
    }
}

const std::vector<const String *> & ReceivedMessage::getChunks(size_t stream_id) const
{
    assert(!is_local);
    if (remaining_consumers != nullptr)
        return fine_grained_chunks[stream_id];
    else
        return chunks;
}

Blocks ReceivedMessage::moveBlocks(size_t stream_id)
{
    assert(is_local);
    if (remaining_consumers != nullptr)
        return std::move(fine_grained_blocks[stream_id]);
    else
        return std::move(blocks);
}

// Constructor that move chunks.
ReceivedMessage::ReceivedMessage(
    size_t source_index_,
    const String & req_info_,
    const std::shared_ptr<DB::TrackedMppDataPacket> & packet_,
    const mpp::Error * error_ptr_,
    const String * resp_ptr_,
    std::vector<const String *> && chunks_,
    size_t fine_grained_consumer_size)
    : source_index(source_index_)
    , req_info(req_info_)
    , packet(packet_)
    , error_ptr(error_ptr_)
    , resp_ptr(resp_ptr_)
    , chunks(chunks_)
{
    if (fine_grained_consumer_size > 0)
    {
        remaining_consumers = std::make_shared<std::atomic<size_t>>(fine_grained_consumer_size);
        fine_grained_chunks.resize(fine_grained_consumer_size);
        if (!chunks.empty())
        {
            RUNTIME_CHECK_MSG(
                !packet->packet.stream_ids().empty(),
                "MPPDataPacket.stream_ids empty, it means ExchangeSender is old version of binary "
                "(source_index: {}) while fine grained shuffle of ExchangeReceiver is enabled. "
                "Cannot handle this.",
                source_index);

            // packet.stream_ids[i] is corresponding to packet.chunks[i],
            // indicating which stream_id this chunk belongs to.
            RUNTIME_CHECK_MSG(
                chunks.size() == static_cast<size_t>(packet->packet.stream_ids_size()),
                "Packet's chunk size({}) not equal to its size of streams({})",
                chunks.size(),
                packet->packet.stream_ids_size());

            for (int i = 0; i < packet->packet.stream_ids_size(); ++i)
            {
                UInt64 stream_id = packet->packet.stream_ids(i) % fine_grained_consumer_size;
                fine_grained_chunks[stream_id].push_back(chunks[i]);
            }
        }
    }
}

// Constructor that move blocks.
ReceivedMessage::ReceivedMessage(
    size_t source_index_,
    const String & req_info_,
    const std::shared_ptr<DB::TrackedMppDataPacket> & packet_,
    Blocks && blocks_,
    size_t fine_grained_consumer_size)
    : source_index(source_index_)
    , req_info(req_info_)
    , packet(packet_)
    , error_ptr(nullptr)
    , resp_ptr(nullptr)
    , blocks(blocks_)
    , is_local(true)
{
    if (fine_grained_consumer_size > 0)
    {
        remaining_consumers = std::make_shared<std::atomic<size_t>>(fine_grained_consumer_size);
        fine_grained_blocks.resize(fine_grained_consumer_size);
        if (!blocks.empty())
        {
            RUNTIME_CHECK_MSG(
                !packet->packet.stream_ids().empty(),
                "MPPDataPacket.stream_ids empty, it means ExchangeSender is old version of binary "
                "(source_index: {}) while fine grained shuffle of ExchangeReceiver is enabled. "
                "Cannot handle this.",
                source_index);

            // packet.stream_ids[i] is corresponding to packet.blocks[i],
            // indicating which stream_id this block belongs to.
            RUNTIME_CHECK_MSG(
                blocks.size() == static_cast<size_t>(packet->packet.stream_ids_size()),
                "Packet's blocks size({}) not equal to its size of streams({})",
                blocks.size(),
                packet->packet.stream_ids_size());

            for (int i = 0; i < packet->packet.stream_ids_size(); ++i)
            {
                UInt64 stream_id = packet->packet.stream_ids(i) % fine_grained_consumer_size;
                fine_grained_blocks[stream_id].push_back(blocks[i]);
            }
        }
    }
}

size_t ReceivedMessage::byteSizeLong() const
{
    if (is_local)
    {
        size_t val = 0;
        for (const auto & block : blocks)
            val += block.bytes();
        return val;
    }
    else
    {
        return getPacket().ByteSizeLong();
    }
}

bool ReceivedMessage::containUsefulMessage() const
{
    return error_ptr != nullptr || resp_ptr != nullptr || (is_local ? !blocks.empty() : !chunks.empty());
}
} // namespace DB
