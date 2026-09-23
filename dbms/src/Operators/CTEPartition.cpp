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
#include <Common/SyncPoint/SyncPoint.h>
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
    auto total_evicted = this->getTotalEvictedBlockNumNoLock();
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

    const auto idx = this->getIdxInMemoryNoLock(cte_reader_id);
    block = this->blocks[idx].block;
    assert(this->blocks[idx].counter > 0);

    if ((--this->blocks[idx].counter) == 0)
    {
        this->memory_usage.fetch_sub(this->blocks[idx].block.bytes());
        this->blocks[idx].block.clear();
    }

    this->addIdxNoLock(cte_reader_id);
    return CTEOpStatus::OK;
}

template <bool for_test>
CTEOpStatus CTEPartition::pushBlock(const Block & block)
{
    std::unique_lock<std::mutex> aux_lock(*(this->aux_lock));
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
    {
#ifndef NDEBUG
        this->cv_for_test->notify_all();
#endif
    }
    else
        this->pipe_cv->notifyAll();


    if unlikely (this->exceedMemoryThreshold())
    {
        this->status = CTEPartitionStatus::NEED_SPILL;
        ret_status = CTEOpStatus::NEED_SPILL;
    }
    return ret_status;
}

bool CTEPartition::needSpill(bool try_mark_need_spill)
{
    std::lock_guard<std::mutex> aux_lock(*(this->aux_lock));
    if (this->status != CTEPartitionStatus::NORMAL)
        return true;
    if (!try_mark_need_spill)
        return false;

    std::lock_guard<std::mutex> lock(*(this->mu));
    if (!this->isSpillTriggeredNoLock() || this->memory_usage.load() == 0)
        return false;

    this->status = CTEPartitionStatus::NEED_SPILL;
    return true;
}

// `fetch_block_idxs` stores the next logical block index for every reader.
// The spill ranges use two coordinate systems. In the example below,
// `evicted_block_num == 10`, so logical index 10 maps to physical index 0:
//
//   logical index:  10       11       12       13       14
//   physical index:  0        1        2        3        4
//   in memory:     [ B10 ]  [ B11 ]  [ B12 ]  [ B13 ]
//
// If a reader is at 10, the range must start at physical index 0. If all
// readers are already past 10, [B10, B<min_reader_idx>) has counter 0 for
// every reader and can be released directly; the first spill range then
// starts at the first reader boundary. Readers at a logical index smaller
// than 10 are still restoring old data from disk and must not be converted
// to a physical index by unsigned subtraction.
//
// Example when every reader has passed the in-memory prefix:
//
//   readers:       A -> 12, B -> 14
//   split_idxs:          { 12 -> 2, 14 -> 4 }
//   released:       [ B10, B11 )
//   spill ranges:                 [ B12, B13 ]
//
// Example when one reader still needs the current memory prefix:
//
//   readers:       A -> 10, B -> 12
//   split_idxs:   { 10 -> 0, 12 -> 2 }
//   spill ranges: [ B10, B11 )  [ B12, ... )
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
            return CTEOpStatus::WAIT_SPILL;
        case CTEPartitionStatus::NEED_SPILL:
            this->status = CTEPartitionStatus::IN_SPILLING;
            break;
        }

        lock.lock();
        this->putTmpBlocksIntoBlocksNoLock();
    }

    // Key represents logical index
    // Value represents physical index in `this->blocks`
    std::map<size_t, size_t> split_idxs;
    const auto evicted_block_num = this->getTotalEvictedBlockNumNoLock();
    bool has_reader_at_or_before_evicted = false;
    for (const auto & [cte_reader_id, logical_idx] : this->fetch_block_idxs)
    {
        if (logical_idx > evicted_block_num)
            split_idxs.insert(std::make_pair(logical_idx, logical_idx - evicted_block_num));
        else
            has_reader_at_or_before_evicted = true;
    }

    size_t released_prefix_num = 0;
    if (has_reader_at_or_before_evicted)
    {
        // The first in-memory block may still be needed by a reader, so the
        // split must start from blocks[0].
        split_idxs.insert(std::make_pair(evicted_block_num, 0));
    }
    else if (!split_idxs.empty())
    {
        // Every reader has passed the prefix before the first split point.
        // Those blocks have counter 0 and can be released without spilling.
        released_prefix_num = split_idxs.begin()->second;
    }
    else
    {
        // This is only possible when there are no readers. Such a partition
        // cannot have a block that needs to be spilled.
        released_prefix_num = this->blocks.size();
    }

    RUNTIME_CHECK(released_prefix_num <= this->blocks.size());
    for (size_t i = 0; i < released_prefix_num; ++i)
        RUNTIME_CHECK(this->blocks[i].counter == 0);

    this->total_block_released_num += released_prefix_num;

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
            end_iter = this->blocks.end();
        else
            end_iter = blocks_begin_iter + next_iter->second;

        while (iter != end_iter)
        {
            RUNTIME_CHECK(iter->counter != 0);
            spilled_blocks.push_back(iter->block);
            ++iter;
        }

        RUNTIME_CHECK(!spilled_blocks.empty());

        this->total_block_in_disk_num += spilled_blocks.size();

        auto spiller = this->config->getSpiller(this->partition_id, this->spillers.size());

        spiller->spillBlocks(std::move(spilled_blocks), this->partition_id);
        spiller->finishSpill();
        this->spillers.insert(std::make_pair(split_iter->first, std::move(spiller)));
        split_iter = next_iter;
    }

    this->blocks.clear();
    this->memory_usage.store(0);

    SYNC_FOR("before_CTEPartition::spillBlocks_merge_tmp_blocks");
    std::lock_guard<std::mutex> aux_lock(*(this->aux_lock));
    this->putTmpBlocksIntoBlocksNoLock();
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

        this->addIdxNoLock(cte_reader_id);
        break;
    };

    return CTEOpStatus::OK;
}

template CTEOpStatus CTEPartition::pushBlock<true>(const Block &);
template CTEOpStatus CTEPartition::pushBlock<false>(const Block &);
} // namespace DB
