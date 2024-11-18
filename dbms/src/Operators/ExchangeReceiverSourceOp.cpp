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

#include <Operators/ExchangeReceiverSourceOp.h>

namespace DB
{
void ExchangeReceiverSourceOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "finish read {} rows from exchange", total_rows);
}

Block ExchangeReceiverSourceOp::popFromBlockQueue()
{
    assert(!block_queue.empty());
    Block block = std::move(block_queue.front());
    block_queue.pop();
    return block;
}

void ExchangeReceiverSourceOp::recordDecodeDetail(
    const DecodeDetail & decode_detail,
    size_t index,
    const String & req_info)
{
    auto & connection_profile_info = io_profile_info->connection_profile_infos[index];
    connection_profile_info.packets += decode_detail.packets;
    connection_profile_info.bytes += decode_detail.packet_bytes;
    total_rows += decode_detail.rows;
    LOG_TRACE(
        log,
        "recv {} rows from exchange receiver for {}, total recv row num: {}",
        decode_detail.rows,
        req_info,
        total_rows);
}

void ExchangeReceiverSourceOp::handleError(const ExchangeReceiverResult & result) const
{
    if (result.meet_error)
    {
        LOG_WARNING(log, "exchange receiver meets error: {}", result.error_msg);
        throw Exception(result.error_msg);
    }
    if (result.resp != nullptr && result.resp->has_error())
    {
        LOG_WARNING(log, "exchange receiver meets error: {}", result.resp->error().DebugString());
        throw Exception(result.resp->error().DebugString());
    }
}

OperatorStatus ExchangeReceiverSourceOp::readImpl(Block & block)
{
    if (!block_queue.empty())
    {
        block = popFromBlockQueue();
        return OperatorStatus::HAS_OUTPUT;
    }

    while (true)
    {
        assert(block_queue.empty());
        ReceivedMessagePtr recv_msg = nullptr;
        auto receive_status = exchange_receiver->tryReceive(stream_id, recv_msg);
        switch (receive_status)
        {
        case ReceiveStatus::empty:
            assert(!recv_msg);
            assert(current_notify_future);
            return OperatorStatus::WAIT_FOR_NOTIFY;
        case ReceiveStatus::ok:
            assert(recv_msg);
        case ReceiveStatus::eof:
            break;
        }

        auto result
            = exchange_receiver
                  ->toExchangeReceiveResult(stream_id, receive_status, recv_msg, block_queue, header, decoder_ptr);

        handleError(result);
        if (result.eof)
        {
            LOG_DEBUG(log, "exchange receiver meets eof");
            return OperatorStatus::HAS_OUTPUT;
        }

        /// only the last response contains execution summaries
        if (result.resp != nullptr)
            io_profile_info->remote_execution_summary.add(*result.resp);

        const auto & decode_detail = result.decode_detail;
        recordDecodeDetail(result.decode_detail, result.call_index, result.req_info);
        if (decode_detail.rows <= 0)
            continue;

        block = popFromBlockQueue();
        return OperatorStatus::HAS_OUTPUT;
    }
}
} // namespace DB
