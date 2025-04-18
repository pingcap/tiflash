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

#include <mutex>
#include <shared_mutex>
#include <utility>
#include "Common/Exception.h"
#include "Core/Block.h"

namespace DB
{
std::pair<Status, Block> CTE::tryGetBlockAt(size_t idx)
{
    std::shared_lock<std::shared_mutex> lock(this->rw_lock, std::defer_lock);
    {
        std::shared_lock<std::shared_mutex> status_lock(this->aux_rw_lock);
        if (this->cte_status != CTE::Normal)
            return {Status::IOOut, Block()};

        // This function is called in cpu pool, we don't want to wait for this lock too long.
        // This lock may be held when spill is in execution. So we need ensure that cte status is not changed
        lock.lock();
    }
    if (this->is_spill_triggered)
    {
        auto spilled_block_num = static_cast<size_t>(this->cte_spill.blockNum());
        if (idx < spilled_block_num)
            return {Status::IOIn, Block()};

        idx -= spilled_block_num;
    }

    auto block_num = this->blocks.size();
    if (block_num <= idx)
    {
        if (this->is_eof)
            return {Status::Eof, Block()};
        else
            return {Status::Waiting, Block()};
    }
    return {Status::Ok, this->blocks[idx]};
}

std::pair<Status, Block> CTE::getBlockFromDisk(size_t idx)
{
    std::shared_lock<std::shared_mutex> lock(this->rw_lock);
    if unlikely (!this->is_spill_triggered)
        // We can call this function only when spill is triggered
        throw Exception("Spill should be triggered");

    if unlikely (static_cast<size_t>(this->cte_spill.blockNum()) <= idx)
        throw Exception("Requested block is not in disk");

    return {Status::Ok, this->cte_spill.readBlockAt(idx)};
}

Status CTE::pushBlock(const Block & block)
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock, std::defer_lock);
    Status ret = Status::Ok;
    {
        std::unique_lock<std::shared_mutex> status_lock(this->aux_rw_lock);
        if (this->cte_status != CTE::Normal)
            return Status::IOOut;


        // This function is called in cpu pool, we don't want to wait for this lock too long.
        // This lock may be held when spill is in execution. So we need ensure that cte status is not changed
        lock.lock();

        this->memory_usage += block.bytes();
        if (this->memory_usage >= this->memory_threshold)
        {
            this->cte_status = CTE::NeedSpill;
            ret = Status::IOOut;
        }
    }

    if unlikely (this->blocks.empty())
        this->pipe_cv.notifyAll();
    this->blocks.push_back(block);
    return ret;
}

void CTE::notifyEOF()
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);
    this->is_eof = true;

    // Just in case someone is in WAITING_FOR_NOTIFY status
    this->pipe_cv.notifyAll();
}

void CTE::spillBlocks()
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);

    this->cte_spill.writeBlocks(this->blocks); // TODO need to handle return value
    this->blocks.resize(0);
    this->memory_usage = 0;

    // TODO handle tmp_blocks

    // Many tasks may be waiting for the finish of spill
    this->pipe_cv.notifyAll();
}

void CTE::registerTask(TaskPtr && task)
{
    // TODO can we directly register the task? Can we ensure that someone must wake it up?
    pipe_cv.registerTask(std::move(task));
}
} // namespace DB
