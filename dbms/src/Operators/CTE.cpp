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
#include <Core/Block.h>
#include <Operators/CTE.h>

#include <cassert>
#include <mutex>
#include <shared_mutex>

#include "Flash/Pipeline/Schedule/Tasks/Task.h"

namespace DB
{
CTEOpStatus CTE::tryGetBlockAt(size_t idx, Block & block)
{
    {
        std::shared_lock<std::shared_mutex> status_lock(this->aux_rw_lock);
        if (this->cte_status != CTE::Normal)
            return Status::IOOut;
    }

    std::shared_lock<std::shared_mutex> lock(this->rw_lock);

    if unlikely (this->is_cancelled)
        return CTEOpStatus::Cancelled;

    if (this->is_spill_triggered)
    {
        auto spilled_block_num = static_cast<size_t>(this->cte_spill.blockNum());
        if (idx < spilled_block_num)
            return CTEOpStatus::IOIn;

        idx -= spilled_block_num;
    }

    auto block_num = this->blocks.size();
    if (block_num <= idx)
        return this->is_eof ? CTEOpStatus::Eof : CTEOpStatus::BlockUnavailable;

    block = this->blocks[idx];
    return CTEOpStatus::Ok;
}

CTEOpStatus CTE::checkAvailableBlock(size_t idx)
{
    {
        std::shared_lock<std::shared_mutex> status_lock(this->aux_rw_lock);
        if (this->cte_status != CTE::Normal)
            return CTEOpStatus::BlockUnavailable;
    }

    std::shared_lock<std::shared_mutex> lock(this->rw_lock);

    if unlikely (this->is_cancelled)
        return CTEOpStatus::Cancelled;

    if (this->is_spill_triggered)
    {
        auto spilled_block_num = static_cast<size_t>(this->cte_spill.blockNum());
        if (idx < spilled_block_num)
            return CTEOpStatus::Ok;

        idx -= spilled_block_num;
    }

    if (this->blocks.size() > idx)
        return CTEOpStatus::Ok;
    return CTEOpStatus::BlockUnavailable;
}

CTEOpStatus CTE::pushBlock(const Block & block)
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock, std::defer_lock);
    CTEOpStatus ret = CTEOpStatus::Ok;
    {
        std::unique_lock<std::shared_mutex> status_lock(this->aux_rw_lock);
        if (this->cte_status != CTE::Normal)
        {
            // Block memory usage will be calculated after the finish of spill
            this->tmp_blocks.push_back(block);
            return CTEOpStatus::IOOut;
        }

        // This function is called in cpu pool, we don't want to wait for this lock too long.
        // This lock may be held when spill is in execution. So we need ensure that cte status is not changed
        lock.lock();

        if unlikely (this->is_cancelled)
            return CTEOpStatus::Cancelled;

        if unlikely (block.rows() == 0)
            // All rows in block may have been filtered and it's needles to store this block
            return CTEOpStatus::Ok;

        this->memory_usage += block.bytes();
        if (this->memory_usage >= this->memory_threshold)
        {
            this->cte_status = CTE::NeedSpill;
            ret = CTEOpStatus::IOOut;
        }
    }

    if unlikely (!this->hasDataNoLock())
        // It's the first time to get block; wake up all tasks that are waiting for blocks.
        this->pipe_cv.notifyAll();
    this->blocks.push_back(block);
    return ret;
}

CTEOpStatus CTE::getBlockFromDisk(size_t idx, Block & block)
{
    std::shared_lock<std::shared_mutex> lock(this->rw_lock);
    if unlikely (this->is_cancelled)
        return CTEOpStatus::Cancelled;

    if unlikely (!this->is_spill_triggered)
        // We can call this function only when spill is triggered
        throw Exception("Spill should be triggered");

    if unlikely (static_cast<size_t>(this->cte_spill.blockNum()) <= idx)
        throw Exception("Requested block is not in disk");

    block = this->cte_spill.readBlockAt(idx);
    return CTEOpStatus::Ok;
}

bool CTE::spillBlocks()
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);

    if unlikely (this->is_cancelled)
        return false;

    while (true)
    {
        this->cte_spill.writeBlocks(this->blocks);
        this->blocks.clear();
        this->memory_usage = 0;

        std::unique_lock<std::shared_mutex> aux_lock(this->aux_rw_lock);
        for (const auto & block : this->tmp_blocks)
        {
            this->blocks.push_back(block);
            this->memory_usage += block.bytes();
        }

        this->tmp_blocks.clear();

        if (this->memory_usage < this->memory_threshold)
        {
            this->cte_status = CTEStatus::Normal;
            break;
        }
    }

    // Many tasks may be waiting for the finish of spill
    this->pipe_cv.notifyAll();
    return true;
}

void CTE::registerTask(TaskPtr && task)
{
    // TODO sometimes we register task because of spill, consider this situation
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        if (!this->hasDataNoLock())
        {
            task->setNotifyType(NotifyType::WAIT_ON_CTE);
            pipe_cv.registerTask(std::move(task));
            return;
        }
    }
    this->pipe_cv.notifyTaskDirectly(std::move(task));
}

CTE::CTEStatus CTE::getStatus()
{
    std::shared_lock<std::shared_mutex> lock(this->aux_rw_lock);
    return this->cte_status;
}
} // namespace DB
