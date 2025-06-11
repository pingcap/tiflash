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
#include <Common/Exception.h>
#include <Core/Block.h>

#include <cassert>
#include <mutex>
#include <shared_mutex>

namespace DB
{
FetchStatus CTE::tryGetBlockAt(size_t idx, Block & block)
{
    std::shared_lock<std::shared_mutex> lock(this->rw_lock);
    if unlikely (this->is_cancelled)
        return Status::Cancelled;

    auto block_num = this->blocks.size();
    if (block_num <= idx)
    {
        if (this->is_eof)
            return {Status::Eof, Block()};
        else
            return {Status::BlockUnavailable, Block()};
    }

    block = this->blocks[idx];
    return FetchStatus::Ok;
}

bool CTE::pushBlock(const Block & block)
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);
    if unlikely (this->is_cancelled)
        return false;

    if unlikely (block.rows() == 0)
        return true;

    if unlikely (this->blocks.empty())
        this->pipe_cv.notifyAll();
    this->blocks.push_back(block);
    return true;
}

void CTE::spillBlocks()
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);

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
            break;
    }

    // Many tasks may be waiting for the finish of spill
    this->pipe_cv.notifyAll();
}

void CTE::registerTask(TaskPtr && task)
{
    {
        std::unique_lock<std::shared_mutex> lock(this->rw_lock);
        if (!this->hasDataNoLock())
        {
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
