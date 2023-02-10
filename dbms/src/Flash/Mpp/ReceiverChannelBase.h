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

#include <memory>

namespace DB
{
namespace FailPoints
{
extern const char random_receiver_local_msg_push_failure_failpoint[];
extern const char random_receiver_sync_msg_push_failure_failpoint[];
extern const char random_receiver_async_msg_push_failure_failpoint[];
} // namespace FailPoints

enum class ReceiverMode
{
    Local = 0,
    Sync,
    Async
};

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
    default:
        throw Exception(fmt::format("Invalid ReceiverMode: {}", magic_enum::enum_name(mode)));
    }
}

inline bool loopJudge(GRPCReceiveQueueRes res) { return (res == GRPCReceiveQueueRes::OK || res == GRPCReceiveQueueRes::FULL); }

// result can not be changed from FULL to OK
inline void updateResult(GRPCReceiveQueueRes & dst, GRPCReceiveQueueRes & src)
{
    if (likely(!(dst == GRPCReceiveQueueRes::FULL && src == GRPCReceiveQueueRes::OK)))
        dst = src;
}

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

struct ReceivedMessage
{
    size_t source_index;
    String req_info;
    // shared_ptr<const MPPDataPacket> is copied to make sure error_ptr, resp_ptr and chunks are valid.
    const std::shared_ptr<DB::TrackedMppDataPacket> packet;
    const mpp::Error * error_ptr;
    const String * resp_ptr;
    std::vector<const String *> chunks;

    // Constructor that move chunks.
    ReceivedMessage(size_t source_index_,
                    const String & req_info_,
                    const std::shared_ptr<DB::TrackedMppDataPacket> & packet_,
                    const mpp::Error * error_ptr_,
                    const String * resp_ptr_,
                    std::vector<const String *> && chunks_)
        : source_index(source_index_)
        , req_info(req_info_)
        , packet(packet_)
        , error_ptr(error_ptr_)
        , resp_ptr(resp_ptr_)
        , chunks(chunks_)
    {}

    void switchMemTracker()
    {
        packet->switchMemTracker(current_memory_tracker);
    }
};

using MsgChannelPtr = std::shared_ptr<MPMCQueue<std::shared_ptr<ReceivedMessage>>>;

class ReceiverChannelBase
{
public:
    ReceiverChannelBase(const size_t channel_size_, const String & req_info_, const LoggerPtr & log_, std::atomic<Int64> * data_size_in_queue_, ReceiverMode mode_)
        : data_size_in_queue(data_size_in_queue_)
        , channel_size(channel_size_)
        , req_info(req_info_)
        , log(log_)
        , mode(mode_)
    {}

    ~ReceiverChannelBase() = default;

protected:
    bool splitPacketIntoChunks(size_t source_index, mpp::MPPDataPacket & packet, std::vector<std::vector<const String *>> & chunks);

    static const mpp::Error * getErrorPtr(const mpp::MPPDataPacket & packet);
    static const String * getRespPtr(const mpp::MPPDataPacket & packet);

    std::atomic<Int64> * data_size_in_queue;
    const size_t channel_size;
    String req_info;
    const LoggerPtr log;
    ReceiverMode mode;
};
} // namespace DB
