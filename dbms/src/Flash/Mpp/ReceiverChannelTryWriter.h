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
class ReceiverChannelTryWriter : public ReceiverChannelBase
{
public:
    ReceiverChannelTryWriter(ReceivedMessageQueue * received_message_queue, const String & req_info_, const LoggerPtr & log_, std::atomic<Int64> * data_size_in_queue_, ReceiverMode mode_)
        : ReceiverChannelBase(received_message_queue, req_info_, log_, data_size_in_queue_, mode_)
    {}

    GRPCReceiveQueueRes tryWrite(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet);

    GRPCReceiveQueueRes tryReWrite();

private:
    GRPCReceiveQueueRes tryWriteImpl(ReceivedMessagePtr & msg);
    GRPCReceiveQueueRes tryRewriteImpl(ReceivedMessagePtr & msg);

    // Push data may fail, so we need to save the message and re-push it at the proper time.
    ReceivedMessagePtr rewrite_msg;
};
} // namespace DB
