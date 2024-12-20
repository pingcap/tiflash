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

#include <Operators/ExchangeSenderSinkOp.h>

namespace DB
{
void ExchangeSenderSinkOp::operatePrefixImpl()
{
    writer->prepare(getHeader());
}

void ExchangeSenderSinkOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "finish write with {} rows", total_rows);
}

OperatorStatus ExchangeSenderSinkOp::writeImpl(Block && block)
{
    assert(!buffer);
    buffer.emplace(std::move(block));
    return tryFlush();
}

OperatorStatus ExchangeSenderSinkOp::tryFlush()
{
    assert(buffer);
    auto res = waitForWriter();
    if (res == OperatorStatus::NEED_INPUT)
    {
        // if writer is ready, write/flush should always succeed
        if (buffer.value())
        {
            total_rows += buffer->rows();
            writer->write(std::move(*buffer));
        }
        else
        {
            // meet eof, flush the writer, and the return status should be FINISHED
            writer->flush();
            res = OperatorStatus::FINISHED;
        }
        buffer.reset();
        return res;
    }
    // writer is not ready, return the status
    return res;
}

OperatorStatus ExchangeSenderSinkOp::waitForWriter() const
{
    auto res = writer->waitForWritable();
    switch (res)
    {
    case WaitResult::Ready:
        return OperatorStatus::NEED_INPUT;
    case WaitResult::WaitForPolling:
        return OperatorStatus::WAITING;
    case WaitResult::WaitForNotify:
        return OperatorStatus::WAIT_FOR_NOTIFY;
    }
}

OperatorStatus ExchangeSenderSinkOp::prepareImpl()
{
    // if there is cached data, flush it
    return buffer ? tryFlush() : OperatorStatus::NEED_INPUT;
}

OperatorStatus ExchangeSenderSinkOp::awaitImpl()
{
    return waitForWriter();
}

} // namespace DB
