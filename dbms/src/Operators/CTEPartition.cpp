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

#include <atomic>
#include <iterator>
#include <mutex>
#include <utility>

namespace DB
{
size_t CTEPartition::getIdxInMemoryNoLock(size_t cte_reader_id)
{
    auto idx = this->fetch_block_idxs[cte_reader_id];
    auto total_evicted = this->getTotalEvictedBlockNumnoLock();
    RUNTIME_CHECK_MSG(
        idx >= total_evicted,
        "partition id: {}, idx: {}, total_evicted: {}",
        this->partition_id,
        idx,
        total_evicted);
    return idx - total_evicted;
}

CTEOpStatus CTEPartition::tryGetBlock(size_t cte_reader_id, Block & block)
{
    std::lock_guard<std::mutex> aux_lock(*(this->aux_lock));
    if (this->status == CTEPartitionStatus::IN_SPILLING)
        return CTEOpStatus::WAIT_SPILL;

    std::lock_guard<std::mutex> lock(*(this->mu));

    this->putTmpBlocksIntoBlocksNoLock();

    if (this->isBlockAvailableInDiskNoLock(cte_reader_id))
        return CTEOpStatus::IO_IN;

    if (!this->isBlockAvailableInMemoryNoLock(cte_reader_id))
        return CTEOpStatus::BLOCK_NOT_AVAILABLE;

    auto idx = this->getIdxInMemoryNoLock(cte_reader_id);
    block = this->blocks[idx].block;
    assert(this->blocks[idx].counter > 0);

    if ((--this->blocks[idx].counter) == 0)
        this->blocks[idx].block.clear();
    // TODO delete -------------
    {
        auto [iter, _] = this->fetch_in_mem_idxs.insert(std::make_pair(cte_reader_id, 0));
        iter->second.push_back(this->fetch_block_idxs[cte_reader_id]);
    }
    // -------------
    this->addIdxNoLock(cte_reader_id);
    return CTEOpStatus::OK;
}

template <bool for_test>
CTEOpStatus CTEPartition::pushBlock(const Block & block)
{
    std::unique_lock<std::mutex> aux_lock(*(this->aux_lock));
    this->total_blocks.fetch_add(1); // TODO delete
    CTEOpStatus ret_status = CTEOpStatus::OK;
    if unlikely (this->status != CTEPartitionStatus::NORMAL)
        this->tmp_blocks.push_back(block);

    switch (this->status)
    {
    case CTEPartitionStatus::NEED_SPILL:
        return CTEOpStatus::NEED_SPILL;
    case CTEPartitionStatus::IN_SPILLING:
        return CTEOpStatus::WAIT_SPILL;
    case CTEPartitionStatus::NORMAL:
        break;
    }

    // mu must be held after aux_lock so that we will not be blocked by spill.
    // Blocked in cpu pool is very bad.
    std::lock_guard<std::mutex> lock(*(this->mu));

    this->memory_usage.fetch_add(block.bytes());
    this->blocks.push_back(BlockWithCounter(block, static_cast<Int16>(this->expected_source_num)));
    if constexpr (for_test)
        this->cv_for_test->notify_all();
    else
        this->pipe_cv->notifyAll();


    if unlikely (this->exceedMemoryThreshold())
    {
        this->status = CTEPartitionStatus::NEED_SPILL;
        ret_status = CTEOpStatus::NEED_SPILL;
    }
    return ret_status;
}

CTEOpStatus CTEPartition::spillBlocks(std::atomic_size_t & block_num, std::atomic_size_t & row_num)
{
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
            this->status = CTEPartitionStatus::IN_SPILLING;
            break;
        }

        lock.lock();
        this->putTmpBlocksIntoBlocksNoLock();
    }

    if (this->first_log)
    {
        // TODO remove
        LOG_INFO(
            this->config->log,
            fmt::format(
                "xzxdebug Partition {} starts cte spill for {}",
                this->partition_id,
                this->config->query_id_and_cte_id));
        this->first_log = false;
    }

    // Key represents logical index
    // Value represents physical index in `this->blocks`
    std::map<size_t, size_t> split_idxs;
    auto evicted_block_num = this->getTotalEvictedBlockNumnoLock();
    split_idxs.insert(std::make_pair(evicted_block_num, 0));
    for (const auto & [cte_reader_id, logical_idx] : this->fetch_block_idxs)
    {
        if (logical_idx > evicted_block_num)
            split_idxs.insert(std::make_pair(logical_idx, logical_idx - evicted_block_num));
    }

    auto split_iter = split_idxs.begin();
    auto blocks_begin_iter = this->blocks.begin();
    auto total_block_in_memory_num = this->blocks.size();
    while (split_iter != split_idxs.end())
    {
        // No more blocks can be spilled
        if (split_iter->second == this->blocks.size())
            break;

        auto next_iter = std::next(split_iter);

        Blocks spilled_blocks;
        auto iter = blocks_begin_iter + split_iter->second;
        decltype(iter) end_iter;
        if (next_iter == split_idxs.end() || next_iter->second >= total_block_in_memory_num)
        {
            this->spill_ranges.push_back(
                std::make_pair(split_iter->first, this->blocks.size() - split_iter->second + split_iter->first));
            end_iter = this->blocks.end();
        }
        else
        {
            this->spill_ranges.push_back(std::make_pair(split_iter->first, next_iter->first));
            end_iter = blocks_begin_iter + next_iter->second;
        }

        bool counter_is_zero = false;
        if (iter->counter == 0)
            // In one slice, all blocks' counter should be 0 or not be 0. Check it.
            counter_is_zero = true;

        while (iter != end_iter)
        {
            if (counter_is_zero)
            {
                RUNTIME_CHECK(iter->counter == 0);
                this->total_block_released_num++;
            }
            else
            {
                RUNTIME_CHECK(iter->counter != 0);
                spilled_blocks.push_back(iter->block);
            }
            ++iter;
        }

        if (counter_is_zero)
        {
            split_iter = next_iter;
            continue;
        }

        RUNTIME_CHECK(!spilled_blocks.empty());

        this->total_block_in_disk_num += spilled_blocks.size();

        auto spiller = this->config->getSpiller(this->partition_id, this->spillers.size());

        // TODO delete -----------------
        this->total_spill_blocks.fetch_add(spilled_blocks.size());
        block_num.fetch_add(spilled_blocks.size());
        for (auto & block : spilled_blocks)
            row_num.fetch_add(block.rows());
        // -----------------

        spiller->spillBlocks(std::move(spilled_blocks), this->partition_id);
        spiller->finishSpill();
        this->spillers.insert(std::make_pair(split_iter->first, std::move(spiller)));
        split_iter = next_iter;
    }

    this->blocks.clear();
    this->memory_usage.store(0);

    std::lock_guard<std::mutex> aux_lock(*(this->aux_lock));
    this->status = CTEPartitionStatus::NORMAL;

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

    bool retried = false;
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
            RUNTIME_CHECK(!retried);

            iter->second->readSuffix();
            iter->second = nullptr;
            retried = true;
            continue;
        }

        // TODO delete -------------
        {
            auto [iter, _] = this->fetch_in_disk_idxs.insert(std::make_pair(cte_reader_id, 0));
            iter->second.push_back(this->fetch_block_idxs[cte_reader_id]);
        }
        // -------------
        this->addIdxNoLock(cte_reader_id);
        break;
    };

    return CTEOpStatus::OK;
}

template CTEOpStatus CTEPartition::pushBlock<true>(const Block &);
template CTEOpStatus CTEPartition::pushBlock<false>(const Block &);
} // namespace DB
