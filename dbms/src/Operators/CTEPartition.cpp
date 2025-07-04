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
#include <Operators/CTEPartition.h>

#include <algorithm>
#include <mutex>
#include <utility>

namespace DB
{
size_t CTEPartition::getIdxInMemoryNoLock(size_t cte_reader_id)
{
    if (this->total_block_in_disk_num >= this->fetch_block_idxs[cte_reader_id])
        return this->fetch_block_idxs[cte_reader_id];
    return this->fetch_block_idxs[cte_reader_id] - this->total_block_in_disk_num;
}

CTEOpStatus CTEPartition::tryGetBlock(size_t cte_reader_id, Block & block)
{
    std::lock_guard<std::mutex> aux_lock(*(this->aux_lock));
    if (this->status == CTEPartitionStatus::IN_SPILLING)
        return CTEOpStatus::IO_OUT;

    std::lock_guard<std::mutex> lock(*this->mu);

    if (this->isBlockAvailableInDiskNoLock(cte_reader_id))
        return CTEOpStatus::IO_IN;

    if (!this->isBlockAvailableInMemoryNoLock(cte_reader_id))
        return CTEOpStatus::BLOCK_NOT_AVAILABLE;

    auto idx = this->getIdxInMemoryNoLock(cte_reader_id);
    block = this->blocks[idx];
    return CTEOpStatus::OK;
}

CTEOpStatus CTEPartition::pushBlock(const Block & block)
{
    std::unique_lock<std::mutex> aux_lock(*(this->aux_lock));
    if (this->status != CTEPartitionStatus::NORMAL)
    {
        if likely (block.rows() != 0)
        {
            // Block memory usage will be calculated after the finish of spill
            this->tmp_blocks.push_back(block);
        }
        return CTEOpStatus::IO_OUT;
    }

    // mu must be held after aux_lock so that we will not be blocked by spill.
    // Blocked in cpu pool is very bad.
    std::lock_guard<std::mutex> lock(*this->mu);

    this->memory_usage += block.bytes();
    this->blocks.push_back(block);
    this->pipe_cv->notifyOne();

    if unlikely (this->exceedMemoryThresholdNoLock())
    {
        this->setCTEPartitionStatusNoLock(CTEPartitionStatus::NEED_SPILL);
        return CTEOpStatus::IO_OUT;
    }
    return CTEOpStatus::OK;
}

CTEOpStatus CTEPartition::spillBlocks()
{
    std::unique_lock<std::mutex> lock(*(this->mu), std::defer_lock);
    {
        std::lock_guard<std::mutex> aux_lock(*(this->aux_lock));
        switch (this->status)
        {
        case CTEPartitionStatus::NORMAL:
            return CTEOpStatus::OK;
        case CTEPartitionStatus::IN_SPILLING:
            return CTEOpStatus::IO_OUT;
        case CTEPartitionStatus::NEED_SPILL:
            this->setCTEPartitionStatusNoLock(CTEPartitionStatus::IN_SPILLING);
            break;
        }

        lock.lock();
        for (const auto & block : this->tmp_blocks)
            this->blocks.push_back(block);
        this->tmp_blocks.clear();
    }

    auto cte_reader_num = this->fetch_block_idxs.size();
    std::vector<size_t> split_idxs{0};
    split_idxs.reserve(cte_reader_num + 1);
    for (auto iter : this->fetch_block_idxs)
        split_idxs.push_back(iter.second);
    std::sort(split_idxs.begin(), split_idxs.end());

    auto begin_iter = this->blocks.begin();
    auto idx_num = split_idxs.size();
    for (size_t i = 0; i < idx_num; i++)
    {
        if (split_idxs[i] >= this->blocks.size())
            break;

        Blocks spilled_blocks;
        if (i == idx_num - 1)
            spilled_blocks.assign(begin_iter + split_idxs[i], this->blocks.end());
        else
            spilled_blocks.assign(begin_iter + split_idxs[i], begin_iter + split_idxs[i + 1]);

        auto spiller = this->spill_context->getSpillAt(i);
        this->spillers.insert(std::make_pair(split_idxs[i], spiller));
        spiller->spillBlocks(std::move(spilled_blocks), this->partition_id);
    }

    this->blocks.clear();
    this->memory_usage = 0;

    std::lock_guard<std::mutex> aux_lock(*(this->aux_lock));
    this->setCTEPartitionStatusNoLock(CTEPartitionStatus::NORMAL);

    // Many tasks may be waiting for the finish of spill
    this->pipe_cv->notifyAll();
    return CTEOpStatus::OK;
}

CTEOpStatus CTEPartition::getBlockFromDisk(size_t cte_reader_id, Block & block)
{
    std::unique_lock<std::mutex> lock(*(this->mu), std::defer_lock);
    {
        std::lock_guard<std::mutex> aux_lock(*(this->aux_lock));
        switch (this->status)
        {
        case CTEPartitionStatus::IN_SPILLING:
            return CTEOpStatus::IO_OUT;
        default:
            break;
        }

        lock.lock();
    }

    RUNTIME_CHECK_MSG(this->isSpillTriggeredNoLock(), "Spill should be triggered");
    RUNTIME_CHECK_MSG(this->isBlockAvailableInDiskNoLock(cte_reader_id), "Requested block is not in disk");

    bool retry = false;
    do
    {
        auto [iter, _] = this->cte_reader_restore_streams.insert(std::make_pair(cte_reader_id, nullptr));
        if (iter->second == nullptr)
        {
            auto spiller_iter = this->spillers.find(this->fetch_block_idxs[cte_reader_id]);
            if (spiller_iter == this->spillers.end())
                // All blocks in disk have been consumed
                return CTEOpStatus::OK;
            auto streams = spiller_iter->second->restoreBlocks(this->partition_id, 1);
            RUNTIME_CHECK(streams.size() == 1);
            iter->second = streams[0];
            iter->second->readPrefix();
        }

        block = iter->second->read();
        if (!block)
        {
            RUNTIME_CHECK(!retry);

            iter->second->readSuffix();
            iter->second = nullptr;
            retry = true;
            continue;
        }

        this->addIdxNoLock(cte_reader_id);
    } while (retry);

    return CTEOpStatus::OK;
}
} // namespace DB
