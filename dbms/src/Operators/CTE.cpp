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
CTEOpStatus CTE::tryGetBlockAt(size_t cte_reader_id, size_t source_id, Block & block)
{
    auto partition_id = this->getPartitionID(source_id);

    {
        std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
        if unlikely (this->is_cancelled)
            return CTEOpStatus::CANCELLED;
    }

    std::lock_guard<std::mutex> status_lock(this->partitions[partition_id].aux_lock);
    if (this->partitions[partition_id].status != CTEPartitionStatus::NORMAL)
        return CTEOpStatus::IO_OUT;

    std::lock_guard<std::mutex> lock(*this->partitions[partition_id].mu);

    if (this->partitions[partition_id].isBlockAvailableInDiskNoLock(cte_reader_id))
        return CTEOpStatus::IO_IN;

    auto status = this->checkBlockAvailableInMemoryNoLock(cte_reader_id, partition_id);
    if (status != CTEOpStatus::OK)
        return status;

    auto idx = this->partitions[partition_id].getIdxInMemoryNoLock(cte_reader_id);
    block = this->partitions[partition_id].blocks[idx];
    return status;
}

CTEOpStatus CTE::checkBlockAvailableInMemoryNoLock(size_t cte_reader_id, size_t partition_id)
{
    if (!this->partitions[partition_id].isBlockAvailableInMemoryNoLock(cte_reader_id))
        return this->is_eof ? CTEOpStatus::END_OF_FILE : CTEOpStatus::BLOCK_NOT_AVAILABLE;

    return CTEOpStatus::OK;
}

CTEOpStatus CTE::pushBlock(size_t sink_id, const Block & block)
{
    if unlikely (block.rows() == 0)
        return CTEOpStatus::OK;

    {
        std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
        if unlikely (this->is_cancelled)
            return CTEOpStatus::CANCELLED;
    }

    auto partition_id = this->getPartitionID(sink_id);
    CTEOpStatus ret = CTEOpStatus::OK;

    std::unique_lock<std::mutex> status_lock(this->partitions[partition_id].aux_lock);
    if (this->partitions[partition_id].status != CTEPartitionStatus::NORMAL)
    {
        if likely (block.rows() != 0)
            // Block memory usage will be calculated after the finish of spill
            this->partitions[partition_id].tmp_blocks.push_back(block);
        return CTEOpStatus::IO_OUT;
    }

    // mu must be held after aux_lock so that we will not be blocked when spill is triggered.
    // Blocked in cpu pool is very bad.
    std::lock_guard<std::mutex> lock(*this->partitions[partition_id].mu);

    this->partitions[partition_id].memory_usage += block.bytes();
    this->partitions[partition_id].blocks.push_back(block);
    this->partitions[partition_id].pipe_cv->notifyOne();

    if unlikely (this->partitions[partition_id].exceedMemoryThresholdNoLock())
    {
        this->partitions[partition_id].setCTEPartitionStatusNoLock(CTEPartitionStatus::NEED_SPILL);
        return CTEOpStatus::IO_OUT;
    }
    return ret;
}

CTEOpStatus CTE::getBlockFromDisk(size_t cte_reader_id, size_t source_id, Block & block)
{
    {
        std::shared_lock<std::shared_mutex> lock(this->rw_lock);
        if unlikely (this->is_cancelled)
        return CTEOpStatus::CANCELLED;
    }
    
    auto partition_id = this->getPartitionID(source_id);


    // TODO
    // block = this->cte_spill.readBlockAt(idx);
    return CTEOpStatus::OK;
}

// TODO maybe CTEPartition should also implement a similiar function
bool CTE::spillBlocks(size_t sink_id)
{
    auto partition_id = this->getPartitionID(sink_id);
    {
        std::shared_lock<std::shared_mutex> lock(this->rw_lock);
        if unlikely (this->is_cancelled)
            return false;
    }

    while (true)
    {
        this->cte_spill.writeBlocks(this->partitions[partition_id].blocks);
        this->partitions[partition_id].blocks.clear();
        this->partitions[partition_id].memory_usage = 0;

        std::unique_lock<std::shared_mutex> aux_lock(this->aux_rw_lock);
        for (const auto & block : this->partitions[partition_id].tmp_blocks)
        {
            this->partitions[partition_id].blocks.push_back(block);
            this->partitions[partition_id].memory_usage += block.bytes();
        }

        this->partitions[partition_id].tmp_blocks.clear();

        // TODO we need to consider total memory usage, not partition memory usage
        if (this->partitions[partition_id].memory_usage < this->memory_threshold)
        {
            this->cte_status = CTEStatus::NORMAL;
            break;
        }
    }

    // Many tasks may be waiting for the finish of spill
    this->partitions[partition_id].pipe_cv->notifyAll();
    return true;
}

// TODO sometimes we register task because of spill, consider this situation
void CTE::checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t cte_reader_id, size_t source_id)
{
    auto partition_id = this->getPartitionID(source_id);
    CTEOpStatus status;

    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
    std::lock_guard<std::mutex> lock(*this->partitions[partition_id].mu);
    status = this->checkBlockAvailableInMemoryNoLock(cte_reader_id, partition_id);

    if (status == CTEOpStatus::BLOCK_NOT_AVAILABLE)
    {
        this->registerTask(partition_id, std::move(task), NotifyType::WAIT_ON_CTE);
        return;
    }

    this->notifyTaskDirectly(partition_id, std::move(task));
}
} // namespace DB
