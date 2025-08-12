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
#include <Operators/CTEPartition.h>

#include <cassert>
#include <mutex>
#include <shared_mutex>

namespace DB
{
CTEOpStatus CTE::tryGetBlockAt(size_t cte_reader_id, size_t partition_id, Block & block)
{
    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);

    if unlikely (!this->areAllSinksRegistered<false>())
        return CTEOpStatus::SINK_NOT_REGISTERED;

    std::lock_guard<std::mutex> lock(*this->partitions[partition_id].mu);

    auto status = this->checkBlockAvailableNoLock(cte_reader_id, partition_id);
    if (status != CTEOpStatus::OK)
        return status;

    auto idx = this->partitions[partition_id].fetch_block_idxs[cte_reader_id]++;
    block = this->partitions[partition_id].blocks[idx].block;
    if ((--this->partitions[partition_id].blocks[idx].counter) == 0)
        this->partitions[partition_id].blocks[idx].block.clear();
    return status;
}

bool CTE::pushBlock(size_t partition_id, const Block & block)
{
    {
        std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
        if unlikely (this->is_cancelled)
            return false;
    }

    if unlikely (block.rows() == 0)
        return true;

    std::lock_guard<std::mutex> lock(*this->partitions[partition_id].mu);
    this->partitions[partition_id].memory_usages += block.bytes();
    this->partitions[partition_id].blocks.push_back(
        BlockWithCounter(block, static_cast<Int16>(this->expected_source_num)));
    this->partitions[partition_id].pipe_cv->notifyAll();
    return true;
}

void CTE::registerTask(size_t partition_id, TaskPtr && task, NotifyType type)
{
    task->setNotifyType(type);
    this->partitions[partition_id].pipe_cv->registerTask(std::move(task));
}

void CTE::checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t cte_reader_id, size_t partition_id)
{
    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
    std::lock_guard<std::mutex> lock(*this->partitions[partition_id].mu);
    CTEOpStatus status = this->checkBlockAvailableNoLock(cte_reader_id, partition_id);

    if (status == CTEOpStatus::BLOCK_NOT_AVAILABLE)
    {
        this->registerTask(partition_id, std::move(task), NotifyType::WAIT_ON_CTE);
        return;
    }

    this->notifyTaskDirectly(partition_id, std::move(task));
}
} // namespace DB
