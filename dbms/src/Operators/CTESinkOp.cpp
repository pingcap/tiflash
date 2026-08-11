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

#include <Operators/CTE.h>
#include <Operators/CTEPartition.h>
#include <Operators/CTESinkOp.h>
#include <Operators/Operator.h>
#include <fmt/core.h>

#include <magic_enum.hpp>

namespace DB
{
void CTESinkOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "finish write with {} rows", this->total_rows);
}

OperatorStatus CTESinkOp::writeImpl(Block && block)
{
    if (!block)
        return OperatorStatus::FINISHED;

    this->total_rows += block.rows();
    auto status = this->cte->pushBlock<false>(this->id, block);
    switch (status)
    {
    case CTEOpStatus::WAIT_SPILL:
        // CTE is spilling blocks to disk, we need to wait the finish of spill
        setNotifyFuture(&(this->io_notifier));
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case CTEOpStatus::NEED_SPILL:
        return OperatorStatus::IO_OUT;
    case CTEOpStatus::OK:
        return OperatorStatus::NEED_INPUT;
    case CTEOpStatus::CANCELLED:
        return OperatorStatus::CANCELLED;
    default:
        throw Exception(fmt::format("Get unexpected CTEOpStatus: {}", magic_enum::enum_name(status)));
    }
}

OperatorStatus CTESinkOp::executeIOImpl()
{
    auto status = this->cte->spillBlocks(this->id);
    switch (status)
    {
    case CTEOpStatus::OK:
        return OperatorStatus::NEED_INPUT;
    case CTEOpStatus::WAIT_SPILL:
        // CTE is spilling blocks to disk, we need to wait the finish of spill
        setNotifyFuture(&(this->io_notifier));
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case CTEOpStatus::CANCELLED:
        return OperatorStatus::CANCELLED;
    default:
        throw Exception(fmt::format("Unexpected status {}", magic_enum::enum_name(status)));
    }
}
} // namespace DB
