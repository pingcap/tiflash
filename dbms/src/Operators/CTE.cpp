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

#include <cassert>
#include <mutex>
#include <shared_mutex>

namespace DB
{
Status CTE::tryGetBlockAt(size_t idx, Block & block)
{
    std::shared_lock<std::shared_mutex> lock(this->rw_lock);
    if unlikely (this->is_cancelled)
        return Status::Cancelled;

    auto block_num = this->blocks.size();
    if (block_num <= idx)
    {
        if (this->is_eof)
            return Status::Eof;
        else
            return Status::Waiting;
    }

    block = this->blocks[idx];
    return Status::Ok;
}

bool CTE::pushBlock(const Block & block)
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);
    if unlikely (this->is_cancelled)
        return false;

    if unlikely (block.rows() == 0)
        return true;

    this->memory_usage += block.bytes();
    if unlikely (this->blocks.empty())
        this->pipe_cv.notifyAll();
    this->blocks.push_back(block);
    return true;
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
