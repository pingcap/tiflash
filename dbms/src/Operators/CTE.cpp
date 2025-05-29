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
#include <deque>
#include <mutex>
#include <shared_mutex>

namespace DB
{
FetchStatus CTE::tryGetBunchBlocks(size_t idx, std::deque<Block> & queue)
{
    assert(queue.empty());

    std::unique_lock<std::shared_mutex> lock(this->rw_lock); // TODO back to shared_lock
    if unlikely (this->is_cancelled)
        return FetchStatus::Cancelled;

    auto block_num = this->blocks.size();
    if (block_num <= idx)
    {
        if (this->is_eof)
        {
            if (!this->first_print)
            {
                auto * log = &Poco::Logger::get("LRUCache");
                LOG_INFO(log, fmt::format("xzxdebug CTE returns eof, block num: {}, row num: {}", this->block_num, this->row_num));
                this->first_print = true;
            }
            return FetchStatus::Eof;
        }
        else
            return FetchStatus::Waiting;
    }

    for (size_t i = idx; i < block_num; i++)
        queue.push_back(this->blocks[i]);
    return FetchStatus::Ok;
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
    this->block_num++;
    this->row_num += block.rows();
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
