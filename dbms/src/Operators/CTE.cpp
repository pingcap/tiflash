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

#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Operators/CTE.h>

#include <cassert>
#include <mutex>
#include <shared_mutex>

namespace DB
{
CTEOpStatus CTE::tryGetBlockAt(size_t idx, Block & block)
{
    std::shared_lock<std::shared_mutex> lock(this->rw_lock);
    auto status = this->checkBlockAvailableNoLock(idx);
    if (status != CTEOpStatus::Ok)
        return status;

    block = this->blocks[idx];
    return status;
}

bool CTE::pushBlock(const Block & block)
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);
    if unlikely (this->is_cancelled)
        return false;

    if unlikely (block.rows() == 0)
        return true;

    this->memory_usage += block.bytes();
    this->blocks.push_back(block);
    this->pipe_cv.notifyOne();
    return true;
}

void CTE::registerTask(TaskPtr && task, NotifyType type)
{
    task->setNotifyType(type);
    pipe_cv.registerTask(std::move(task));
}

void CTE::checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t expected_block_fetch_idx)
{
    std::shared_lock<std::shared_mutex> shared_lock(this->rw_lock);
    CTEOpStatus status = this->checkBlockAvailableNoLock(expected_block_fetch_idx);
    if (status == CTEOpStatus::Ok)
    {
        this->notifyTaskDirectly(std::move(task));
        return;
    }

    this->registerTask(std::move(task), NotifyType::WAIT_ON_CTE);
}
} // namespace DB
