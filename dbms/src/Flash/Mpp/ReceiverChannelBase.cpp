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

#include <Flash/Mpp/ReceiverChannelBase.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <utility>

namespace DB
{
bool ReceiverChannelBase::splitPacketIntoChunks(size_t source_index, mpp::MPPDataPacket & packet, std::vector<std::vector<const String *>> & chunks)
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
        UInt64 stream_id = packet.stream_ids(i) % channel_size;
        chunks[stream_id].push_back(&packet.chunks(i));
    }

    return true;
}

const mpp::Error * ReceiverChannelBase::getErrorPtr(const mpp::MPPDataPacket & packet)
{
    if (unlikely(packet.has_error()))
        return &packet.error();
    return nullptr;
}

const String * ReceiverChannelBase::getRespPtr(const mpp::MPPDataPacket & packet)
{
    if (unlikely(!packet.data().empty()))
        return &packet.data();
    return nullptr;
}
} // namespace DB
