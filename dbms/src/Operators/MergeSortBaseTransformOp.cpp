// Copyright 2023 PingCAP, Ltd.
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

#include <Core/CachedSpillHandler.h>
#include <Core/SpillHandler.h>
#include <DataStreams/MergeSortingBlocksBlockInputStream.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <DataStreams/SortHelper.h>
#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Interpreters/sortBlock.h>
#include <Operators/MergeSortBaseTransformOp.h>

#include <magic_enum.hpp>

namespace DB
{
template<bool do_partial_sort>
void MergeSortBaseTransformOp<do_partial_sort>::operatePrefix()
{
    header_without_constants = getHeader();
    SortHelper::removeConstantsFromBlock(header_without_constants);
    SortHelper::removeConstantsFromSortDescription(header, order_desc);
    // For order by constants, generate LimitOperator instead of SortOperator.
    assert(!order_desc.empty());

    spiller = std::make_unique<Spiller>(spill_config, true, 1, header_without_constants, log);
}

template<bool do_partial_sort>
void MergeSortBaseTransformOp<do_partial_sort>::operateSuffix()
{
    if likely (merge_impl)
        merge_impl->readSuffix();
}

template<bool do_partial_sort>
Block MergeSortBaseTransformOp<do_partial_sort>::getMergeOutput()
{
    assert(merge_impl);
    Block block = merge_impl->read();
    if likely (block)
        SortHelper::enrichBlockWithConstants(block, header);
    return block;
}

template<bool do_partial_sort>
OperatorStatus MergeSortBaseTransformOp<do_partial_sort>::fromPartialToMerge(Block & block)
{
    assert(status == LocalSortStatus::PARTIAL);
    // convert to merge phase.
    status = LocalSortStatus::MERGE;
    if likely (!sorted_blocks.empty())
    {
        // In merge phase, the MergeSortingBlocksBlockInputStream of pull model is used to do merge sort.
        // TODO refine MergeSortingBlocksBlockInputStream and use a more common class to do merge sort in both push model and pull model.
        merge_impl = std::make_unique<MergeSortingBlocksBlockInputStream>(
            sorted_blocks,
            order_desc,
            log->identifier(),
            max_block_size,
            limit);
        merge_impl->readPrefix();
        block = getMergeOutput();
    }
    return OperatorStatus::HAS_OUTPUT;
}

template<bool do_partial_sort>
OperatorStatus MergeSortBaseTransformOp<do_partial_sort>::fromPartialToRestore()
{
    assert(status == LocalSortStatus::PARTIAL);
    // convert to restore phase.
    status = LocalSortStatus::RESTORE;

    LOG_INFO(log, "Begin restore data from disk for merge sort.");

    /// Create spilled sorted streams to merge.
    spiller->finishSpill();
    auto inputs_to_merge = spiller->restoreBlocks(0, 0);

    /// Rest of sorted_blocks in memory.
    if (!sorted_blocks.empty())
        inputs_to_merge.emplace_back(std::make_shared<MergeSortingBlocksBlockInputStream>(
            sorted_blocks,
            order_desc,
            log->identifier(),
            max_block_size,
            limit));

    /// merge the spilled data and memory data.
    merge_impl = std::make_unique<MergingSortedBlockInputStream>(inputs_to_merge, order_desc, max_block_size, limit);
    merge_impl->readPrefix();
    return OperatorStatus::IO;
}

template<bool do_partial_sort>
OperatorStatus MergeSortBaseTransformOp<do_partial_sort>::fromPartialToSpill()
{
    assert(status == LocalSortStatus::PARTIAL);
    // convert to restore phase.
    status = LocalSortStatus::SPILL;
    assert(!cached_handler);
    if (!spiller->hasSpilledData())
        LOG_INFO(log, "Begin spill in local sort");
    cached_handler = spiller->createCachedSpillHandler(
        std::make_shared<MergeSortingBlocksBlockInputStream>(sorted_blocks, order_desc, log->identifier(), max_block_size, limit),
        /*partition_id=*/0,
        [&]() { return exec_status.isCancelled(); });
    // fallback to partial phase.
    if (!cached_handler->batchRead())
        return fromSpillToPartial();
    return OperatorStatus::IO;
}

template<bool do_partial_sort>
OperatorStatus MergeSortBaseTransformOp<do_partial_sort>::fromSpillToPartial()
{
    assert(status == LocalSortStatus::SPILL);
    assert(cached_handler);
    cached_handler.reset();
    sum_bytes_in_blocks = 0;
    sorted_blocks.clear();
    status = LocalSortStatus::PARTIAL;
    return OperatorStatus::NEED_INPUT;
}

template<bool do_partial_sort>
OperatorStatus MergeSortBaseTransformOp<do_partial_sort>::transformImpl(Block & block)
{
    switch (status)
    {
    case LocalSortStatus::PARTIAL:
    {
        if unlikely (!block)
        {
            return spiller->hasSpilledData()
                ? fromPartialToRestore()
                : fromPartialToMerge(block);
        }

        // execute partial sort and store the sorted block in `sorted_blocks`.
        SortHelper::removeConstantsFromBlock(block);
        if constexpr (do_partial_sort)
        {
            sortBlock(block, order_desc, limit);
        }
        sum_bytes_in_blocks += block.estimateBytesForSpill();
        sorted_blocks.emplace_back(std::move(block));

        if (max_bytes_before_external_sort && sum_bytes_in_blocks > max_bytes_before_external_sort)
            return fromPartialToSpill();
        else
            return OperatorStatus::NEED_INPUT;
    }
    default:
        throw Exception(fmt::format("Unexpected status: {}.", magic_enum::enum_name(status)));
    }
}

template<bool do_partial_sort>
OperatorStatus MergeSortBaseTransformOp<do_partial_sort>::tryOutputImpl(Block & block)
{
    switch (status)
    {
    case LocalSortStatus::PARTIAL:
        return OperatorStatus::NEED_INPUT;
    case LocalSortStatus::SPILL:
    {
        assert(cached_handler);
        return cached_handler->batchRead()
            ? OperatorStatus::IO
            : fromSpillToPartial();
    }
    case LocalSortStatus::MERGE:
    {
        if likely (merge_impl)
            block = getMergeOutput();
        return OperatorStatus::HAS_OUTPUT;
    }
    case LocalSortStatus::RESTORE:
    {
        if (restored_result.hasData())
        {
            block = restored_result.output();
            return OperatorStatus::HAS_OUTPUT;
        }
        return OperatorStatus::IO;
    }
    default:
        throw Exception(fmt::format("Unexpected status: {}.", magic_enum::enum_name(status)));
    }
}

template<bool do_partial_sort>
OperatorStatus MergeSortBaseTransformOp<do_partial_sort>::executeIOImpl()
{
    switch (status)
    {
    case LocalSortStatus::SPILL:
    {
        assert(cached_handler);
        cached_handler->spill();
        return OperatorStatus::NEED_INPUT;
    }
    case LocalSortStatus::RESTORE:
    {
        restored_result.put(getMergeOutput());
        return OperatorStatus::HAS_OUTPUT;
    }
    default:
        throw Exception(fmt::format("Unexpected status: {}.", magic_enum::enum_name(status)));
    }
}

template<bool do_partial_sort>
void MergeSortBaseTransformOp<do_partial_sort>::transformHeaderImpl(Block &)
{
}

template<bool do_partial_sort>
bool MergeSortBaseTransformOp<do_partial_sort>::RestoredResult::hasData() const
{
    return finished || block.has_value();
}

template<bool do_partial_sort>
void MergeSortBaseTransformOp<do_partial_sort>::RestoredResult::put(Block && ret)
{
    assert(!hasData());
    if unlikely (!ret)
        finished = true;
    block.emplace(std::move(ret));
}

template<bool do_partial_sort>
Block MergeSortBaseTransformOp<do_partial_sort>::RestoredResult::output()
{
    if unlikely (finished)
        return {};
    Block ret = std::move(*block);
    block.reset();
    return ret;
}

template class MergeSortBaseTransformOp<true>;
template class MergeSortBaseTransformOp<false>;

} // namespace DB
