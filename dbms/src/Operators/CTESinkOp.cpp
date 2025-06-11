// Copyright 2025 PingCAP, Inc.
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

#include <Operators/CTESinkOp.h>
#include <Operators/Operator.h>
#include <fmt/core.h>

namespace DB
{
void CTESinkOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "finish write with {} rows", this->total_rows);
}

OperatorStatus CTESinkOp::writeImpl(Block && block)
{
    if (!block)
    {
        return OperatorStatus::FINISHED;
    }
    this->total_rows += block.rows();
    auto status = this->cte->pushBlock(block);
    switch (status)
    {
    case Status::IOOut:
        return OperatorStatus::IO_OUT;
    case Status::Ok:
        return OperatorStatus::NEED_INPUT;
    default:
        throw Exception(fmt::format("Get unexpected Status: {}", magic_enum::enum_name(status)));
    }
}

OperatorStatus CTESinkOp::executeIOImpl()
{
    if likely (this->cte->spillBlocks())
        return OperatorStatus::NEED_INPUT;
    return OperatorStatus::CANCELLED;
}

OperatorStatus CTESinkOp::awaitImpl()
{
    // CTESinkOp waits for the finish of spill
    if (this->cte->getStatus() == CTE::CTEStatus::Normal)
        return OperatorStatus::NEED_INPUT;
    return OperatorStatus::WAITING;
}
} // namespace DB
