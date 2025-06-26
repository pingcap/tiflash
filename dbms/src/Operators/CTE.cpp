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

#include <cassert>
#include <mutex>
#include <shared_mutex>

namespace DB
{
CTEOpStatus CTE::tryGetBlockAt(size_t cte_reader_id, size_t source_id, Block & block)
{
    {
        std::shared_lock<std::shared_mutex> status_lock(this->aux_rw_lock);
        if (this->cte_status != CTEStatus::NORMAL)
            return CTEOpStatus::IO_OUT;
    }

    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);

    if unlikely (this->is_cancelled)
        return CTEOpStatus::CANCELLED;

    auto partition_id = this->getPartitionID(source_id);
    std::lock_guard<std::mutex> lock(*this->partitions[partition_id].mu);

    if (this->is_spill_triggered)
    {
        // TODO need refinement
        // auto spilled_block_num = static_cast<size_t>(this->cte_spill.blockNum());
        // if (idx < spilled_block_num)
        //     return CTEOpStatus::IOIn;

    }


    auto status = this->checkBlockAvailableNoLock(cte_reader_id, partition_id);
    if (status != CTEOpStatus::OK)
        return status;

    auto idx = this->partitions[partition_id].fetch_block_idxs[cte_reader_id].idx++;
    block = this->partitions[partition_id].blocks[idx];
    return status;
}

CTEOpStatus CTE::checkBlockAvailableNoLock(size_t cte_reader_id, size_t partition_id)
{
    {
        std::shared_lock<std::shared_mutex> status_lock(this->aux_rw_lock);
        if (this->cte_status != CTEStatus::NORMAL)
            return CTEOpStatus::BLOCK_NOT_AVAILABLE;
    }

    if unlikely (this->is_cancelled)
        return CTEOpStatus::CANCELLED;

    if (this->is_spill_triggered)
    {
        // TODO judge if block is in the disk
        // auto spilled_block_num = static_cast<size_t>(this->cte_spill.blockNum());
        // if (idx < spilled_block_num)
        //     return CTEOpStatus::OK;

        // idx -= spilled_block_num;
    }

    if (this->partitions[partition_id].blocks.size()
        <= this->partitions[partition_id].fetch_block_idxs[cte_reader_id].idx)
        return this->is_eof ? CTEOpStatus::END_OF_FILE : CTEOpStatus::BLOCK_NOT_AVAILABLE;

    return CTEOpStatus::OK;
}

CTEOpStatus CTE::pushBlock(size_t sink_id, const Block & block)
{
    auto partition_id = this->getPartitionID(sink_id);
    CTEOpStatus ret = CTEOpStatus::OK;
    {
        std::unique_lock<std::shared_mutex> status_lock(this->aux_rw_lock);
        if (this->cte_status != CTEStatus::NORMAL)
        {
            if likely (block.rows() != 0)
                // Block memory usage will be calculated after the finish of spill
                this->partitions[partition_id].tmp_blocks.push_back(block);
            return CTEOpStatus::IO_OUT;
        }
    }

    // This function is called in cpu pool, we don't want to wait for this lock too long.
    // This lock may be held when spill is in execution. So we need ensure that cte status is not changed
    std::unique_lock<std::shared_mutex> rw_lock(this->rw_lock);

    if unlikely (this->is_cancelled)
        return CTEOpStatus::CANCELLED;

    if unlikely (block.rows() == 0)
        // All rows in block may have been filtered and it's needles to store this block
        return CTEOpStatus::OK;

    std::lock_guard<std::mutex> lock(*this->partitions[partition_id].mu);
    this->partitions[partition_id].memory_usage += block.bytes();
    this->partitions[partition_id].blocks.push_back(block);
    this->partitions[partition_id].pipe_cv->notifyOne();

    // TODO memory usage judgement should be considered from global perspective
    // if (this->partitions[partition_id].memory_usage >= this->memory_threshold)
    // {
    //     this->cte_status = CTE::NEED_SPILL;
    //     ret = CTEOpStatus::IO_OUT;
    // }
    return ret;
}

CTEOpStatus CTE::getBlockFromDisk(size_t source_id, size_t idx, Block & block)
{
    auto partition_id = this->getPartitionID(source_id);
    
    std::shared_lock<std::shared_mutex> lock(this->rw_lock);
    if unlikely (this->is_cancelled)
        return CTEOpStatus::CANCELLED;

    // We can call this function only when spill is triggered
    RUNTIME_CHECK_MSG(this->is_spill_triggered, "Spill should be triggered");

    // TODO
    RUNTIME_CHECK_MSG(static_cast<size_t>(this->cte_spill.blockNum()) <= idx, "Requested block is not in disk");

    // TODO
    // block = this->cte_spill.readBlockAt(idx);
    return CTEOpStatus::OK;
}

// TODO maybe CTEPartition should also implement a similiar function
bool CTE::spillBlocks(size_t sink_id)
{
    auto partition_id = this->getPartitionID(sink_id);
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);

    if unlikely (this->is_cancelled)
        return false;

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
    status = this->checkBlockAvailableNoLock(cte_reader_id, partition_id);

    if (status == CTEOpStatus::BLOCK_NOT_AVAILABLE)
    {
        this->registerTask(partition_id, std::move(task), NotifyType::WAIT_ON_CTE);
        return;
    }

    this->notifyTaskDirectly(partition_id, std::move(task));
}

CTE::CTEStatus CTE::getStatus()
{
    std::shared_lock<std::shared_mutex> lock(this->aux_rw_lock);
    return this->cte_status;
}
} // namespace DB
