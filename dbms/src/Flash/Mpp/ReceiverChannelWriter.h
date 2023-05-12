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
#include <Common/LooseBoundedMPMCQueue.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <Flash/Mpp/ReceiverChannelBase.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>

#include <memory>

namespace DB
{
class ReceiverChannelWriter : public ReceiverChannelBase
{
public:
    ReceiverChannelWriter(std::vector<MsgChannelPtr> * msg_channels_, const String & req_info_, const LoggerPtr & log_, std::atomic<Int64> * data_size_in_queue_, ReceiverMode mode_)
        : ReceiverChannelBase(msg_channels_->size(), req_info_, log_, data_size_in_queue_, mode_)
        , msg_channels(msg_channels_)
    {}

    // "write" means writing the packet to the channel which is a LooseBoundedMPMCQueue.
    //
    // If is_force:
    //    call LooseBoundedMPMCQueue::forcePush
    // If !is_force:
    //    call LooseBoundedMPMCQueue::push
    //
    // If enable_fine_grained_shuffle:
    //      Seperate chunks according to packet.stream_ids[i], then push to msg_channels[stream_id].
    // If fine grained_shuffle is disabled:
    //      Push all chunks to msg_channels[0].
    //
    // Return true if all push succeed, otherwise return false.
    // NOTE: shared_ptr<MPPDataPacket> will be hold by all ExchangeReceiverBlockInputStream to make chunk pointer valid.
    template <bool enable_fine_grained_shuffle, bool is_force = false>
    bool write(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
    {
        const auto & packet = tracked_packet->packet;
        const mpp::Error * error_ptr = packet.has_error() ? &packet.error() : nullptr;
        const String * resp_ptr = packet.data().empty() ? nullptr : &packet.data();

        WriteToChannelFunc write_func;
        if constexpr (is_force)
            write_func = [&](size_t i, ReceivedMessagePtr && recv_msg) {
                return (*msg_channels)[i]->forcePush(std::move(recv_msg));
            };
        else
            write_func = [&](size_t i, ReceivedMessagePtr && recv_msg) {
                return (*msg_channels)[i]->push(std::move(recv_msg));
            };

        bool success;
        if constexpr (enable_fine_grained_shuffle)
            success = writeFineGrain(write_func, source_index, tracked_packet, error_ptr, resp_ptr);
        else
            success = writeNonFineGrain(write_func, source_index, tracked_packet, error_ptr, resp_ptr);

        if (likely(success))
            ExchangeReceiverMetric::addDataSizeMetric(*data_size_in_queue, tracked_packet->getPacket().ByteSizeLong());
        LOG_TRACE(log, "push recv_msg to msg_channels(size: {}) succeed:{}, enable_fine_grained_shuffle: {}", msg_channels->size(), success, enable_fine_grained_shuffle);
        return success;
    }

    bool isReadyForWrite() const;

private:
    using WriteToChannelFunc = std::function<MPMCQueueResult(size_t, ReceivedMessagePtr &&)>;

    bool writeFineGrain(
        WriteToChannelFunc write_func,
        size_t source_index,
        const TrackedMppDataPacketPtr & tracked_packet,
        const mpp::Error * error_ptr,
        const String * resp_ptr);
    bool writeNonFineGrain(
        WriteToChannelFunc write_func,
        size_t source_index,
        const TrackedMppDataPacketPtr & tracked_packet,
        const mpp::Error * error_ptr,
        const String * resp_ptr);

    std::vector<MsgChannelPtr> * msg_channels;
};
} // namespace DB
