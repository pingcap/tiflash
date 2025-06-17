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
#include <Operators/CTE.h>
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

    auto res = this->cte_reader->fetchNextBlock();
    switch (res.first)
    {
    case CTEOpStatus::Eof:
        this->cte_reader->getResp(this->resp);
        if (this->resp.execution_summaries_size() != 0)
            this->io_profile_info->remote_execution_summary.add(this->resp);
    case CTEOpStatus::Ok:
        block = res.second;
        this->total_rows += block.rows();
        return OperatorStatus::HAS_OUTPUT;
    case CTEOpStatus::IOIn:
        // Expected block is in disk, we need to read it from disk
        return OperatorStatus::IO_IN;
    case CTEOpStatus::IOOut:
    {
        // CTE is spilling blocks to disk, we need to wait the finish of spill
        this->wait_type = CTESourceOp::Spill;
        return OperatorStatus::WAITING;
    }
    case CTEOpStatus::BlockUnavailable:
        if likely (this->cte_reader->isBlockGenerated())
        {
            return OperatorStatus::WAITING;
        }
        else
        {
            // CTE has not begun to receive data yet
            // So we need to wait the notify from CTE
            this->cte_reader->setNotifyFuture();
            return OperatorStatus::WAIT_FOR_NOTIFY;
        }
    case CTEOpStatus::Cancelled:
        return OperatorStatus::CANCELLED;
    }
}

OperatorStatus CTESourceOp::executeIOImpl()
{
    RUNTIME_CHECK(!this->block_from_disk);
    auto status = this->cte_reader->fetchBlockFromDisk(this->block_from_disk);
    switch (status)
    {
    case CTEOpStatus::Ok:
        return OperatorStatus::HAS_OUTPUT;
    case CTEOpStatus::Cancelled:
        return OperatorStatus::CANCELLED;
    default:
        throw Exception(fmt::format("Get unexpected status {}", magic_enum::enum_name(status)));
    }
}

OperatorStatus CTESourceOp::awaitImpl()
{
    if (this->wait_type == CTESourceOp::WaitType::NeedMoreBlock)
    {
        auto res = this->cte_reader->checkAvailableBlock();
        switch (res)
        {
        case CTEOpStatus::BlockUnavailable:
            return OperatorStatus::WAITING;
        case CTEOpStatus::Ok:
        case CTEOpStatus::Eof:
            return OperatorStatus::HAS_OUTPUT;
        case CTEOpStatus::Cancelled:
            return OperatorStatus::CANCELLED;
        default:
            throw Exception(fmt::format("Get unexpected status {}", magic_enum::enum_name(res)));
        }
    }
    else if (this->wait_type == CTESourceOp::WaitType::Spill)
    {
        switch (this->cte_reader->getCTEStatus())
        {
        case CTE::CTEStatus::Normal:
            return OperatorStatus::HAS_OUTPUT;
        case CTE::CTEStatus::NeedSpill:
        case CTE::CTEStatus::InSpilling:
            return OperatorStatus::WAITING;
        }
    }
    else
    {
        throw Exception(fmt::format("Unexpected wait type {}", magic_enum::enum_name(this->wait_type)));
    }
}
} // namespace DB
