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
#include <Common/MPMCQueue.h>
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

    // "write" means writing the packet to the channel which is a MPMCQueue.
    //
    // If enable_fine_grained_shuffle:
    //      Seperate chunks according to packet.stream_ids[i], then push to msg_channels[stream_id].
    // If fine grained_shuffle is disabled:
    //      Push all chunks to msg_channels[0].
    // Return true if all push succeed, otherwise return false.
    // NOTE: shared_ptr<MPPDataPacket> will be hold by all ExchangeReceiverBlockInputStream to make chunk pointer valid.
    template <bool enable_fine_grained_shuffle>
    bool write(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet);

private:
    bool writeFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr);
    bool writeNonFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr);

    std::vector<MsgChannelPtr> * msg_channels;
};
} // namespace DB
