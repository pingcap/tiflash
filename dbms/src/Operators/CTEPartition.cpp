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

#include <iterator>
#include <mutex>
#include <utility>

namespace DB
{
size_t CTEPartition::getIdxInMemoryNoLock(size_t cte_reader_id)
{
    RUNTIME_CHECK(this->fetch_block_idxs[cte_reader_id] >= this->total_block_in_disk_num);
    return this->fetch_block_idxs[cte_reader_id] - this->total_block_in_disk_num;
}

CTEOpStatus CTEPartition::tryGetBlock(size_t cte_reader_id, Block & block)
{
    std::lock_guard<std::mutex> aux_lock(*(this->aux_lock));
    if (this->status == CTEPartitionStatus::IN_SPILLING)
        return CTEOpStatus::WAIT_SPILL;

    std::lock_guard<std::mutex> lock(*this->mu);

    if (this->isBlockAvailableInDiskNoLock(cte_reader_id))
        return CTEOpStatus::IO_IN;

    if (!this->isBlockAvailableInMemoryNoLock(cte_reader_id))
        return CTEOpStatus::BLOCK_NOT_AVAILABLE;

    auto idx = this->getIdxInMemoryNoLock(cte_reader_id);
    block = this->blocks[idx];
    this->addIdxNoLock(cte_reader_id);
    {
        auto [iter, _] = this->total_fetch_block_nums.insert(std::make_pair(cte_reader_id, 0));
        iter->second++;
    }
    {
        auto [iter, _] = this->total_fetch_row_nums.insert(std::make_pair(cte_reader_id, 0));
        iter->second += block.rows();
    }
    return CTEOpStatus::OK;
}

CTEOpStatus CTEPartition::pushBlock(const Block & block)
{
    std::unique_lock<std::mutex> aux_lock(*(this->aux_lock));
    CTEOpStatus ret_status = CTEOpStatus::OK;
    switch (this->status)
    {
    case CTEPartitionStatus::NEED_SPILL:
        ret_status = CTEOpStatus::NEED_SPILL;
    case CTEPartitionStatus::IN_SPILLING:
        ret_status = CTEOpStatus::WAIT_SPILL;
        if likely (block.rows() != 0)
            // Block memory usage will be calculated after the finish of spill
            this->tmp_blocks.push_back(block);
        return ret_status;
    case CTEPartitionStatus::NORMAL:
        break;
    }

    // mu must be held after aux_lock so that we will not be blocked by spill.
    // Blocked in cpu pool is very bad.
    std::lock_guard<std::mutex> lock(*this->mu);

    this->total_recv_block_num++;
    this->total_recv_row_num += block.rows();
    this->total_byte_usage += block.bytes();

    this->memory_usage += block.bytes();
    this->blocks.push_back(block);
    this->pipe_cv->notifyOne();

    if unlikely (this->exceedMemoryThresholdNoLock())
    {
        this->setCTEPartitionStatusNoLock(CTEPartitionStatus::NEED_SPILL);
        ret_status = CTEOpStatus::NEED_SPILL;
    }
    return ret_status;
}

CTEOpStatus CTEPartition::spillBlocks()
{
    LOG_INFO(
        this->spill_context->getLog(),
        fmt::format(
            "Partition {} starts cte spill for {}",
            this->partition_id,
            this->spill_context->getQueryIdAndCTEId()));
    std::unique_lock<std::mutex> lock(*(this->mu), std::defer_lock);
    {
        std::lock_guard<std::mutex> aux_lock(*(this->aux_lock));
        switch (this->status)
        {
        case CTEPartitionStatus::NORMAL:
            return CTEOpStatus::OK;
        case CTEPartitionStatus::IN_SPILLING:
            return CTEOpStatus::WAIT_SPILL;
        case CTEPartitionStatus::NEED_SPILL:
            this->setCTEPartitionStatusNoLock(CTEPartitionStatus::IN_SPILLING);
            break;
        }

        lock.lock();
        for (const auto & block : this->tmp_blocks)
        {
            this->memory_usage += block.bytes();
            this->blocks.push_back(block);
        }
        this->tmp_blocks.clear();
    }

    // Key represents logical index
    // Value represents physical index at `this->blocks`
    std::map<size_t, size_t> split_idxs;
    split_idxs.insert(std::make_pair(this->total_block_in_disk_num, 0));
    for (const auto & [_, logical_idx] : this->fetch_block_idxs)
        if (logical_idx > this->total_block_in_disk_num)
            split_idxs.insert(std::make_pair(logical_idx, logical_idx - this->total_block_in_disk_num));

    auto blocks_begin_iter = this->blocks.begin();
    auto split_iter = split_idxs.begin();
    auto total_block_in_memory_num = this->blocks.size();
    while (split_iter != split_idxs.end())
    {
        if (split_iter->second == this->blocks.size())
            break;

        auto next_iter = std::next(split_iter);

        Blocks spilled_blocks;
        if (next_iter == split_idxs.end() || next_iter->second >= total_block_in_memory_num)
            spilled_blocks.assign(blocks_begin_iter + split_iter->second, this->blocks.end());
        else
            spilled_blocks.assign(blocks_begin_iter + split_iter->second, blocks_begin_iter + next_iter->second);

        RUNTIME_CHECK(!spilled_blocks.empty());

        this->total_block_in_disk_num += spilled_blocks.size();

        this->total_spill_block_num += spilled_blocks.size(); // TODO remove

        auto spiller = this->spill_context->getSpiller(this->partition_id, this->spillers.size());
        spiller->spillBlocks(std::move(spilled_blocks), this->partition_id);
        spiller->finishSpill();
        this->spillers.insert(std::make_pair(split_iter->first, std::move(spiller)));
        split_iter++;
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
        if (this->status == CTEPartitionStatus::IN_SPILLING)
            return CTEOpStatus::WAIT_SPILL;

        lock.lock();
    }

    RUNTIME_CHECK_MSG(this->isSpillTriggeredNoLock(), "Spill should be triggered");
    RUNTIME_CHECK_MSG(this->isBlockAvailableInDiskNoLock(cte_reader_id), "Requested block is not in disk");

    bool retry = false;
    while (true)
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
        {
            auto [iter, _] = this->fetch_idxs_disk.insert(std::make_pair(cte_reader_id, std::vector<size_t>{}));
            iter->second.push_back(this->fetch_block_idxs[cte_reader_id]);
        }
        this->addIdxNoLock(cte_reader_id);
        {
            auto [iter, _] = this->total_fetch_disk_block_nums.insert(std::make_pair(cte_reader_id, 0));
            iter->second++;
        }
        {
            auto [iter, _] = this->total_fetch_disk_row_nums.insert(std::make_pair(cte_reader_id, 0));
            iter->second += block.rows();
        }
        break;
    };

    return CTEOpStatus::OK;
}
} // namespace DB
