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

ReturnOpStatus ExchangeSenderSinkOp::writeImpl(Block && block)
{
    if (!block)
    {
        writer->flush();
        return OperatorStatus::FINISHED;
    }

    total_rows += block.rows();
    writer->write(block);
    return OperatorStatus::NEED_INPUT;
}

ReturnOpStatus ExchangeSenderSinkOp::prepareImpl()
{
    return writer->isWritable() ? OperatorStatus::NEED_INPUT : OperatorStatus::WAITING;
}

ReturnOpStatus ExchangeSenderSinkOp::awaitImpl()
{
    return writer->isWritable() ? OperatorStatus::NEED_INPUT : OperatorStatus::WAITING;
}

} // namespace DB
