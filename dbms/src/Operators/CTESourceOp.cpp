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

#include <Common/Exception.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Operators/CTE.h>
#include <Operators/CTEPartition.h>
#include <Operators/CTESourceOp.h>
#include <Operators/Operator.h>

namespace DB
{
void CTESourceOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "finish read {} rows from cte source", total_rows);
}

OperatorStatus CTESourceOp::readImpl(Block & block)
{
    if (this->block_from_disk)
    {
        block = this->block_from_disk;
        this->block_from_disk.clear();
        return OperatorStatus::HAS_OUTPUT;
    }

    auto ret = this->cte_reader->fetchNextBlock(this->id, block);
    switch (ret)
    {
    case CTEOpStatus::END_OF_FILE:
        if (this->cte_reader->getResp(this->resp))
            this->io_profile_info->remote_execution_summary.add(this->resp);
    case CTEOpStatus::OK:
        this->total_rows += block.rows();
        return OperatorStatus::HAS_OUTPUT;
    case CTEOpStatus::IO_IN:
        // Expected block is in disk, we need to read it from disk
        return OperatorStatus::IO_IN;
    case CTEOpStatus::WAIT_SPILL:
        // CTE is spilling blocks to disk, we need to wait the finish of spill
        DB::setNotifyFuture(&(this->io_notifier));
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case CTEOpStatus::CANCELLED:
        return OperatorStatus::CANCELLED;
    case CTEOpStatus::BLOCK_NOT_AVAILABLE:
        DB::setNotifyFuture(&(this->notifier));
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case CTEOpStatus::SINK_NOT_REGISTERED:
        this->sw.start();
        return OperatorStatus::WAITING;
    default:
        throw Exception("Should not reach here");
    }
}

OperatorStatus CTESourceOp::executeIOImpl()
{
    RUNTIME_CHECK(!this->block_from_disk);
    auto status = this->cte_reader->fetchBlockFromDisk(this->id, this->block_from_disk);
    switch (status)
    {
    case CTEOpStatus::OK:
        return OperatorStatus::HAS_OUTPUT;
    case CTEOpStatus::WAIT_SPILL:
        // CTE is spilling blocks to disk, we need to wait the finish of spill
        DB::setNotifyFuture(&(this->io_notifier));
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case CTEOpStatus::CANCELLED:
        return OperatorStatus::CANCELLED;
    default:
        throw Exception(fmt::format("Get unexpected status {}", magic_enum::enum_name(status)));
    }
}
} // namespace DB
