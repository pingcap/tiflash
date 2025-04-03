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

namespace DB
{
FetchStatus CTE::checkAvailableBlockAt(size_t idx)
{
    std::shared_lock<std::shared_mutex> lock(this->rw_lock);
    auto block_num = this->blocks.size(); // TODO consider spill
    if (block_num <= idx)
    {
        if (this->is_eof)
            return FetchStatus::Eof;
        else
            return FetchStatus::Waiting;
    }
    // TODO handle FetchStatus::Cancelled
    return FetchStatus::Ok;
}

std::pair<FetchStatus, Block> CTE::tryGetBlockAt(size_t idx)
{
    std::shared_lock<std::shared_mutex> lock(this->rw_lock);
    auto block_num = this->blocks.size(); // TODO maybe blocks are in disk
    if (block_num <= idx)
    {
        if (this->is_eof)
            return {FetchStatus::Eof, Block()};
        else
            return {FetchStatus::Waiting, Block()};
    }
    // TODO handle error and cancel
    // TODO maybe fetch block from disk
    return {FetchStatus::Ok, this->blocks[idx]};
}

void CTE::pushBlock(const Block & block)
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);

    // TODO consider spill
    this->memory_usage += block.bytes();
    // TODO check spill
    if unlikely (this->blocks.empty())
        this->pipe_cv.notifyAll();
    this->blocks.push_back(block);
}

void CTE::notifyEOF()
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);
    this->is_eof = true;

    // Just in case someone is in WAITING_FOR_NOTIFY status
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
} // namespace DB
