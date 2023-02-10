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
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <Flash/Mpp/ReceiverChannelBase.h>

#include <memory>

namespace DB
{
class ReceiverChannelTryWriter : public ReceiverChannelBase
{
public:
    ReceiverChannelTryWriter(std::vector<GRPCReceiveQueue<ReceivedMessage>> * grpc_recv_queues_, const String & req_info_, const LoggerPtr & log_, std::atomic<Int64> * data_size_in_queue_, ReceiverMode mode_)
        : ReceiverChannelBase(grpc_recv_queues_->size(), req_info_, log_, data_size_in_queue_, mode_)
        , grpc_recv_queues(grpc_recv_queues_)
    {}

    template <bool enable_fine_grained_shuffle>
    GRPCReceiveQueueRes tryWrite(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet);

    template <bool enable_fine_grained_shuffle>
    GRPCReceiveQueueRes tryReWrite();

private:
    GRPCReceiveQueueRes tryWriteFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr);
    GRPCReceiveQueueRes tryWriteNonFineGrain(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet, const mpp::Error * error_ptr, const String * resp_ptr);

    GRPCReceiveQueueRes tryWriteImpl(size_t index, std::shared_ptr<ReceivedMessage> && msg);
    GRPCReceiveQueueRes tryRewriteImpl(size_t index, std::shared_ptr<ReceivedMessage> & msg);

    // Because of the deleted copy constructor, we have to type it as pointer.
    std::vector<GRPCReceiveQueue<ReceivedMessage>> * grpc_recv_queues;

    // Push data may fail, so we need to save the message and re-push it at the proper time.
    std::map<size_t, std::shared_ptr<ReceivedMessage>> rewrite_msgs;
};
} // namespace DB
