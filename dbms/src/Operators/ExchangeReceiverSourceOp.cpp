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

#include <Operators/ExchangeReceiverSourceOp.h>

namespace DB
{
void ExchangeReceiverSourceOp::operateSuffix() noexcept
{
    LOG_DEBUG(log, "finish read {} rows from remote", total_rows);
}

OperatorStatus ExchangeReceiverSourceOp::readImpl(Block & block)
{
    if (!block_queue.empty())
    {
        std::swap(block, block_queue.front());
        block_queue.pop();
        return OperatorStatus::HAS_OUTPUT;
    }

    auto await_status = awaitImpl();
    if (await_status == OperatorStatus::HAS_OUTPUT)
    {
        std::swap(block, block_queue.front());
        block_queue.pop();
        return OperatorStatus::HAS_OUTPUT;
    }
    return await_status;
}

OperatorStatus ExchangeReceiverSourceOp::awaitImpl()
{
    if (!block_queue.empty() || recv_msg)
        return OperatorStatus::HAS_OUTPUT;
    while (true)
    {
        if (remote_reader->receive(recv_msg, stream_id))
        {
            if (recv_msg->chunks.empty())
                return OperatorStatus::FINISHED;
            auto result = remote_reader->toDecodeResult(block_queue, header, recv_msg, decoder_ptr);
            recv_msg.reset();
            if (result.meet_error)
            {
                LOG_WARNING(log, "remote reader meets error: {}", result.error_msg);
                throw Exception(result.error_msg);
            }

            if (result.resp != nullptr && result.resp->has_error())
            {
                LOG_WARNING(log, "remote reader meets error: {}", result.resp->error().DebugString());
                throw Exception(result.resp->error().DebugString());
            }

            auto decode_detail = result.decode_detail;
            total_rows += decode_detail.rows;
            LOG_TRACE(
                log,
                "recv {} rows from remote for {}, total recv row num: {}",
                decode_detail.rows,
                result.req_info,
                total_rows);
            return OperatorStatus::HAS_OUTPUT;
        }
        // else continue
    }
}
} // namespace DB
