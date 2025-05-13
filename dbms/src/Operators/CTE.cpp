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

#include <deque>
#include <mutex>
#include <shared_mutex>
#include <utility>

namespace DB
{
FetchStatus CTE::tryGetBunchBlocks(size_t idx, std::deque<Block> & queue)
{
    std::shared_lock<std::shared_mutex> lock(this->rw_lock);
    auto block_num = this->blocks.size();
    if (block_num <= idx)
    {
        if (this->is_eof)
            return FetchStatus::Eof;
        else
            return FetchStatus::Waiting;
    }

    for (size_t i = 0; i < block_num; i++)
        queue.push_back(this->blocks[i]);
    return FetchStatus::Ok;
}

void CTE::pushBlock(const Block & block)
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);

    this->memory_usage += block.bytes();
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
