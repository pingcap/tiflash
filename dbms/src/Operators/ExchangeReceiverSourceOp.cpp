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
void ExchangeReceiverSourceOp::operateSuffix()
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
        auto await_status = awaitImpl();
        if (await_status == OperatorStatus::HAS_OUTPUT)
        {
            assert(recv_res);
            assert(recv_res->recv_status != ReceiveStatus::empty);
            auto result = exchange_receiver->toExchangeReceiveResult(
                *recv_res,
                block_queue,
                header,
                decoder_ptr);
            recv_res.reset();

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
            if (result.eof)
            {
                LOG_DEBUG(log, "exchange receiver meets eof");
                return OperatorStatus::HAS_OUTPUT;
            }

            const auto & decode_detail = result.decode_detail;
            total_rows += decode_detail.rows;
            LOG_TRACE(
                log,
                "recv {} rows from exchange receiver for {}, total recv row num: {}",
                decode_detail.rows,
                result.req_info,
                total_rows);

            if (decode_detail.rows <= 0)
                continue;

            block = popFromBlockQueue();
            return OperatorStatus::HAS_OUTPUT;
        }
        assert(!recv_res);
        return await_status;
    }
}

OperatorStatus ExchangeReceiverSourceOp::awaitImpl()
{
    if (!block_queue.empty() || recv_res)
        return OperatorStatus::HAS_OUTPUT;
    recv_res.emplace(exchange_receiver->nonBlockingReceive(stream_id));
    switch (recv_res->recv_status)
    {
    case ReceiveStatus::ok:
        assert(recv_res->recv_msg);
        return OperatorStatus::HAS_OUTPUT;
    case ReceiveStatus::empty:
        recv_res.reset();
        return OperatorStatus::WAITING;
    case ReceiveStatus::eof:
        return OperatorStatus::HAS_OUTPUT;
    }
}
} // namespace DB
