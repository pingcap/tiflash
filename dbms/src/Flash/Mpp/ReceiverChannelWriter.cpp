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

#include <Common/Exception.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <Flash/Mpp/ReceiverChannelWriter.h>

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
    case ReceiverMode::Async:
        fiu_do_on(FailPoints::random_receiver_async_msg_push_failure_failpoint, push_succeed = false);
        break;
    default:
        RUNTIME_ASSERT(false, "Illegal ReceiverMode");
    }
}
} // namespace

bool ReceiverChannelWriter::writeFineGrain(
    WriteToChannelFunc write_func,
    size_t source_index,
    const TrackedMppDataPacketPtr & tracked_packet,
    const mpp::Error * error_ptr,
    const String * resp_ptr)
{
    bool success = true;
    auto & packet = tracked_packet->packet;
    std::vector<std::vector<const String *>> chunks(msg_channels->size());

    if (!splitFineGrainedShufflePacketIntoChunks(source_index, packet, chunks))
        return false;

    // Still need to send error_ptr or resp_ptr even if packet.chunks_size() is zero.
    for (size_t i = 0; i < msg_channels->size() && success; ++i)
    {
        if (resp_ptr == nullptr && error_ptr == nullptr && chunks[i].empty())
            continue;

        auto recv_msg = std::make_shared<ReceivedMessage>(
            source_index,
            req_info,
            tracked_packet,
            error_ptr,
            resp_ptr,
            std::move(chunks[i]));
        success = (write_func(i, std::move(recv_msg)) == MPMCQueueResult::OK);

        injectFailPointReceiverPushFail(success, mode);

        // Only the first ExchangeReceiverInputStream need to handle resp.
        resp_ptr = nullptr;
    }
    return success;
}

bool ReceiverChannelWriter::writeNonFineGrain(
    WriteToChannelFunc write_func,
    size_t source_index,
    const TrackedMppDataPacketPtr & tracked_packet,
    const mpp::Error * error_ptr,
    const String * resp_ptr)
{
    bool success = true;
    auto & packet = tracked_packet->packet;
    std::vector<const String *> chunks(packet.chunks_size());

    for (int i = 0; i < packet.chunks_size(); ++i)
        chunks[i] = &packet.chunks(i);

    if (!(resp_ptr == nullptr && error_ptr == nullptr && chunks.empty()))
    {
        auto recv_msg = std::make_shared<ReceivedMessage>(
            source_index,
            req_info,
            tracked_packet,
            error_ptr,
            resp_ptr,
            std::move(chunks));

        success = write_func(0, std::move(recv_msg)) == MPMCQueueResult::OK;
        injectFailPointReceiverPushFail(success, mode);
    }
    return success;
}

bool ReceiverChannelWriter::isWritable() const
{
    for (const auto & msg_channel : *msg_channels)
    {
        if (msg_channel->isFull())
            return false;
    }
    return true;
}
} // namespace DB
