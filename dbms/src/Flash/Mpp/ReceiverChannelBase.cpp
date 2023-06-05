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
#include <Flash/Mpp/ReceiverChannelBase.h>

#include <utility>

namespace DB
{
namespace
{
const mpp::Error * getErrorPtr(const mpp::MPPDataPacket & packet)
{
    if (unlikely(packet.has_error()))
        return &packet.error();
    return nullptr;
}

const String * getRespPtr(const mpp::MPPDataPacket & packet)
{
    if (unlikely(!packet.data().empty()))
        return &packet.data();
    return nullptr;
}
} // namespace

ReceivedMessagePtr toReceivedMessage(
    const TrackedMppDataPacketPtr & tracked_packet,
    size_t source_index,
    const String & req_info,
    bool for_fine_grained_shuffle,
    size_t fine_grained_consumer_size)
{
    const auto & packet = tracked_packet->packet;
    const mpp::Error * error_ptr = getErrorPtr(packet);
    const String * resp_ptr = getRespPtr(packet);
    std::vector<const String *> chunks(packet.chunks_size());
    for (int i = 0; i < packet.chunks_size(); ++i)
        chunks[i] = &packet.chunks(i);
    return std::make_shared<ReceivedMessage>(
        source_index,
        req_info,
        tracked_packet,
        error_ptr,
        resp_ptr,
        std::move(chunks),
        for_fine_grained_shuffle,
        fine_grained_consumer_size);
}
} // namespace DB
