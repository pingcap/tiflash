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
#include <Flash/Mpp/ReceivedMessageQueue.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>

#include <memory>

namespace DB
{
namespace ExchangeReceiverMetric
{
inline void addDataSizeMetric(std::atomic<Int64> & data_size_in_queue, size_t size)
{
    data_size_in_queue.fetch_add(size);
    GET_METRIC(tiflash_exchange_queueing_data_bytes, type_receive).Increment(size);
}

inline void subDataSizeMetric(std::atomic<Int64> & data_size_in_queue, size_t size)
{
    data_size_in_queue.fetch_sub(size);
    GET_METRIC(tiflash_exchange_queueing_data_bytes, type_receive).Decrement(size);
}

inline void clearDataSizeMetric(std::atomic<Int64> & data_size_in_queue)
{
    GET_METRIC(tiflash_exchange_queueing_data_bytes, type_receive).Decrement(data_size_in_queue.load());
}
} // namespace ExchangeReceiverMetric

ReceivedMessagePtr toReceivedMessage(
    const TrackedMppDataPacketPtr & tracked_packet,
    size_t source_index,
    const String & req_info,
    bool for_fine_grained_shuffle,
    size_t fine_grained_consumer_size);

class ReceiverChannelBase
{
public:
    ReceiverChannelBase(ReceivedMessageQueue * received_message_queue_, const String & req_info_, const LoggerPtr & log_, std::atomic<Int64> * data_size_in_queue_, ReceiverMode mode_)
        : data_size_in_queue(data_size_in_queue_)
        , received_message_queue(received_message_queue_)
        , fine_grained_channel_size(received_message_queue->getFineGrainedStreamSize())
        , enable_fine_grained_shuffle(fine_grained_channel_size > 0)
        , req_info(req_info_)
        , log(log_)
        , mode(mode_)
    {}

    ~ReceiverChannelBase() = default;

protected:
    std::atomic<Int64> * data_size_in_queue;
    ReceivedMessageQueue * received_message_queue = nullptr;
    size_t fine_grained_channel_size;
    bool enable_fine_grained_shuffle;
    String req_info;
    const LoggerPtr log;
    ReceiverMode mode;
};
} // namespace DB
