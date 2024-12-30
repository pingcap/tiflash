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

#include <Flash/Coprocessor/DAGResponseWriter.h>
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
    assert(!writer->hasPendingFlush());
    WriteResult write_res;
    OperatorStatus ret = block ? OperatorStatus::NEED_INPUT : OperatorStatus::FINISHED;
    if (block)
    {
        // write
        write_res = writer->write(std::move(block));
    }
    else
    {
        write_res = writer->flush();
    }
    if (write_res != WriteResult::Done)
    {
        assert(writer->hasPendingFlush());
        ret = write_res == WriteResult::NeedWaitForPolling ? OperatorStatus::WAITING : OperatorStatus::WAIT_FOR_NOTIFY;
    }
    return ret;
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
    if (writer->hasPendingFlush())
    {
        // if there is pending flush, flush it
        auto res = writer->flush();
        if (res == WriteResult::Done)
        {
            assert(!writer->hasPendingFlush());
            return OperatorStatus::NEED_INPUT;
        }
        else
        {
            assert(writer->hasPendingFlush());
            return res == WriteResult::NeedWaitForPolling ? OperatorStatus::WAITING : OperatorStatus::WAIT_FOR_NOTIFY;
        }
    }
    return OperatorStatus::NEED_INPUT;
}

OperatorStatus ExchangeSenderSinkOp::awaitImpl()
{
    // awaitImpl should only be called if there is pending flush
    RUNTIME_CHECK(writer->hasPendingFlush());
    return waitForWriter();
}

} // namespace DB
