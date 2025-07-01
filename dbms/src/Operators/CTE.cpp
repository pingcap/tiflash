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
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
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
    if unlikely (this->is_cancelled)
        return CTEOpStatus::CANCELLED;

    auto status = this->partitions[partition_id].tryGetBlockAt(cte_reader_id, block);
    switch (status)
    {
    case CTEOpStatus::BLOCK_NOT_AVAILABLE:
        return this->is_eof ? CTEOpStatus::END_OF_FILE : CTEOpStatus::BLOCK_NOT_AVAILABLE;
    default:
        return status;
    }
}

CTEOpStatus CTE::pushBlock(size_t partition_id, const Block & block)
{
    if unlikely (block.rows() == 0)
        return CTEOpStatus::OK;

    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
    if unlikely (this->is_cancelled)
        return CTEOpStatus::CANCELLED;

    return this->partitions[partition_id].pushBlock(block);
}

CTEOpStatus CTE::getBlockFromDisk(size_t cte_reader_id, size_t partition_id, Block & block)
{
    {
        std::shared_lock<std::shared_mutex> lock(this->rw_lock);
        if unlikely (this->is_cancelled)
            return CTEOpStatus::CANCELLED;
    }

    return this->partitions[partition_id].getBlockFromDisk(cte_reader_id, block);
}

CTEOpStatus CTE::spillBlocks(size_t partition_id)
{
    {
        std::shared_lock<std::shared_mutex> lock(this->rw_lock);
        if unlikely (this->is_cancelled)
            return CTEOpStatus::CANCELLED;
    }

    return this->partitions[partition_id].spillBlocks();
}

void CTE::checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t cte_reader_id, size_t partition_id)
{
    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
    if (this->is_cancelled)
    {
        this->notifyTaskDirectly(partition_id, std::move(task));
        return;
    }

    std::lock_guard<std::mutex> status_lock(*(this->partitions[partition_id].status_lock));
    if (this->partitions[partition_id].status == CTEPartitionStatus::IN_SPILLING)
    {
        this->notifyTaskDirectly(partition_id, std::move(task));
        return;
    }

    std::lock_guard<std::mutex> lock(*(this->partitions[partition_id].mu));
    if (this->partitions[partition_id].isBlockAvailableNoLock(cte_reader_id) || this->is_eof)
        this->notifyTaskDirectly(partition_id, std::move(task));
    else
        this->registerTask(partition_id, std::move(task), NotifyType::WAIT_ON_CTE);
}

void CTE::checkInSpillingAndRegisterTask(TaskPtr && task, size_t partition_id)
{
    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
    if (this->is_cancelled)
    {
        this->notifyTaskDirectly(partition_id, std::move(task));
        return;
    }

    std::lock_guard<std::mutex> status_lock(*(this->partitions[partition_id].status_lock));
    if (this->partitions[partition_id].status == CTEPartitionStatus::IN_SPILLING)
        this->registerTask(partition_id, std::move(task), NotifyType::WAIT_ON_CTE);
    else
        this->notifyTaskDirectly(partition_id, std::move(task));
}
} // namespace DB
