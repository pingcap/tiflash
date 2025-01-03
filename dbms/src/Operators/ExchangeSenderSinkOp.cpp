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
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Mpp/Utils.h>
#include <Operators/ExchangeSenderSinkOp.h>

namespace DB
{
void ExchangeSenderSinkOp::operatePrefixImpl()
{
    setHeader(getHeaderByMppVersion(getHeader(), exec_context.getMppVersion()));
    writer->prepare(getHeader());
}

void ExchangeSenderSinkOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "finish write with {} rows", total_rows);
}

OperatorStatus ExchangeSenderSinkOp::writeImpl(Block && block)
{
    assert(!writer->hasPendingFlush());
    if (block)
    {
        total_rows += block.rows();
        // write
        return writeResultToOperatorStatus(writer->write(std::move(block)));
    }
    // input is done
    input_done = true;
    return writeResultToOperatorStatus(writer->flush());
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

OperatorStatus ExchangeSenderSinkOp::writeResultToOperatorStatus(WriteResult res) const
{
    if (res == WriteResult::Done)
    {
        assert(!writer->hasPendingFlush());
        return input_done ? OperatorStatus::FINISHED : OperatorStatus::NEED_INPUT;
    }
    assert(writer->hasPendingFlush());
    return res == WriteResult::NeedWaitForPolling ? OperatorStatus::WAITING : OperatorStatus::WAIT_FOR_NOTIFY;
}

OperatorStatus ExchangeSenderSinkOp::prepareImpl()
{
    if (writer->hasPendingFlush())
    {
        // if there is pending flush, flush it
        return writeResultToOperatorStatus(writer->flush());
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
