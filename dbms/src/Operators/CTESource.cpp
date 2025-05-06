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
#include <Operators/CTESource.h>
#include <Operators/Operator.h>

namespace DB
{
void CTESourceOp::operateSuffixImpl()
{
    this->cte.reset();
    this->cte_manager->releaseCTE(this->query_id_and_cte_id);
    LOG_DEBUG(log, "finish read {} rows from cte source", total_rows);
}

OperatorStatus CTESourceOp::readImpl(Block & block)
{
    if (this->block_from_disk)
    {
        block = this->block_from_disk;
        this->block_from_disk = Block();
        return OperatorStatus::HAS_OUTPUT;
    }

    auto res = this->cte->tryGetBlockAt(this->block_fetch_idx);
    switch (res.first)
    {
    case Status::Eof:
    case Status::Ok:
        block = res.second;
        ++(this->block_fetch_idx);
        return OperatorStatus::HAS_OUTPUT;
    case Status::IOIn:
        // Expected block is in disk, we need to read it from disk
        return OperatorStatus::IO_IN;
    case Status::IOOut:
        {
            // CTE is spilling blocks to disk, we need to wait the finish of spill
            this->wait_type = CTESourceOp::Spill;
            return OperatorStatus::WAITING;
        }
    case Status::BlockUnavailable:
        if unlikely (this->block_fetch_idx == 0)
            // CTE has not begun to receive data yet when block_fetch_idx == 0
            // So we need to wait the notify from CTE
            return OperatorStatus::WAIT_FOR_NOTIFY;
        else
        {
            // CTE not have enough block, we need to wait for it
            this->wait_type = CTESourceOp::NeedMoreBlock;
            return OperatorStatus::WAITING;
        }
    case Status::Cancelled:
        return OperatorStatus::CANCELLED;
    }
}

OperatorStatus CTESourceOp::executeIOImpl()
{
    this->block_from_disk = this->cte->getBlockFromDisk(this->block_fetch_idx);
    this->block_fetch_idx++;
    return OperatorStatus::HAS_OUTPUT;
}

OperatorStatus CTESourceOp::awaitImpl()
{
    if (this->wait_type == CTESourceOp::WaitType::NeedMoreBlock)
    {
        auto ret = this->cte->tryGetBlockAt(this->block_fetch_idx);
        switch (ret.first)
        {
        case Status::IOOut:
            this->wait_type = CTESourceOp::WaitType::Spill;
        case Status::BlockUnavailable:
            return OperatorStatus::WAITING;
        case Status::Ok:
        case Status::Eof:
            return OperatorStatus::HAS_OUTPUT;
        case Status::Cancelled:
            return OperatorStatus::CANCELLED;
        case Status::IOIn:
            return OperatorStatus::IO_IN;
        }
    }
    else if (this->wait_type == CTESourceOp::WaitType::Spill)
    {
        switch (this->cte->getStatus())
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
        throw Exception("Unexpected wait type");
    }
}
} // namespace DB
