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
const std::vector<const String *> & ReceivedMessage::getChunks(size_t stream_id) const
{
    if (fine_grained_consumer_size > 0)
        return fine_grained_chunks[stream_id];
    else
        return chunks;
}
// Constructor that move chunks.
ReceivedMessage::ReceivedMessage(
    size_t source_index_,
    const String & req_info_,
    const std::shared_ptr<DB::TrackedMppDataPacket> & packet_,
    const mpp::Error * error_ptr_,
    const String * resp_ptr_,
    std::vector<const String *> && chunks_,
    size_t fine_grained_consumer_size_)
    : source_index(source_index_)
    , req_info(req_info_)
    , packet(packet_)
    , error_ptr(error_ptr_)
    , resp_ptr(resp_ptr_)
    , chunks(chunks_)
    , remaining_consumers(fine_grained_consumer_size_)
    , fine_grained_consumer_size(fine_grained_consumer_size_)
{
    if (fine_grained_consumer_size > 0)
    {
        fine_grained_chunks.resize(fine_grained_consumer_size);
        if (packet->packet.chunks_size() > 0)
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
                packet->packet.chunks_size() == packet->packet.stream_ids_size(),
                "Packet's chunk size({}) not equal to its size of streams({})",
                packet->packet.chunks_size(),
                packet->packet.stream_ids_size());

            for (int i = 0; i < packet->packet.stream_ids_size(); ++i)
            {
                UInt64 stream_id = packet->packet.stream_ids(i) % fine_grained_consumer_size;
                fine_grained_chunks[stream_id].push_back(&packet->packet.chunks(i));
            }
        }
    }
}
bool ReceivedMessage::containUsefulMessage() const
{
    return error_ptr != nullptr || resp_ptr != nullptr || !chunks.empty();
}
} // namespace DB
