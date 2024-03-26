// Copyright 2023 PingCAP, Inc.
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
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Operators/MergeSortTransformOp.h>

#include <magic_enum.hpp>

namespace DB
{
void MergeSortTransformOp::operatePrefixImpl()
{
    header_without_constants = getHeader();
    SortHelper::removeConstantsFromBlock(header_without_constants);
    SortHelper::removeConstantsFromSortDescription(header, order_desc);
    // For order by constants, generate LimitOperator instead of SortOperator.
    assert(!order_desc.empty());

    if (sort_spill_context->isSpillEnabled())
        sort_spill_context->buildSpiller(header_without_constants);
}

void MergeSortTransformOp::operateSuffixImpl()
{
    if likely (merge_impl)
        merge_impl->readSuffix();
}

Block MergeSortTransformOp::getMergeOutput()
{
    assert(merge_impl);
    Block block = merge_impl->read();
    if likely (block)
        SortHelper::enrichBlockWithConstants(block, header);
    return block;
}

ReturnOpStatus MergeSortTransformOp::fromPartialToMerge(Block & block)
{
    assert(status == MergeSortStatus::PARTIAL);
    // convert to merge phase.
    status = MergeSortStatus::MERGE;
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

ReturnOpStatus MergeSortTransformOp::fromPartialToRestore()
{
    assert(status == MergeSortStatus::PARTIAL);
    // convert to restore phase.
    status = MergeSortStatus::RESTORE;

    LOG_INFO(log, "Begin restore data from disk for merge sort.");

    /// Create spilled sorted streams to merge.
    sort_spill_context->getSpiller()->finishSpill();
    auto inputs_to_merge = sort_spill_context->getSpiller()->restoreBlocks(0, 0);

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
    return OperatorStatus::IO_IN;
}

ReturnOpStatus MergeSortTransformOp::fromPartialToSpill()
{
    assert(status == MergeSortStatus::PARTIAL);
    // convert to restore phase.
    status = MergeSortStatus::SPILL;
    assert(!cached_handler);
    sort_spill_context->markSpilled();
    cached_handler = sort_spill_context->getSpiller()->createCachedSpillHandler(
        std::make_shared<MergeSortingBlocksBlockInputStream>(
            sorted_blocks,
            order_desc,
            log->identifier(),
            std::max(1, max_block_size / 10),
            limit),
        /*partition_id=*/0,
        [&]() { return exec_context.isCancelled(); });
    // fallback to partial phase.
    if (!cached_handler->batchRead())
        return fromSpillToPartial();
    return OperatorStatus::IO_OUT;
}

ReturnOpStatus MergeSortTransformOp::fromSpillToPartial()
{
    assert(status == MergeSortStatus::SPILL);
    assert(cached_handler);
    cached_handler.reset();
    sum_bytes_in_blocks = 0;
    sorted_blocks.clear();
    status = MergeSortStatus::PARTIAL;
    sort_spill_context->finishOneSpill();
    return OperatorStatus::NEED_INPUT;
}

ReturnOpStatus MergeSortTransformOp::transformImpl(Block & block)
{
    switch (status)
    {
    case MergeSortStatus::PARTIAL:
    {
        if unlikely (!block)
        {
            sort_spill_context->finishSpillableStage();
            if (!sorted_blocks.empty() && sort_spill_context->needFinalSpill())
                return fromPartialToSpill();
            return hasSpilledData() ? fromPartialToRestore() : fromPartialToMerge(block);
        }

        // store the sorted block in `sorted_blocks`.
        SortHelper::removeConstantsFromBlock(block);
        sum_bytes_in_blocks += block.estimateBytesForSpill();
        sorted_blocks.emplace_back(std::move(block));

        if (sort_spill_context->updateRevocableMemory(sum_bytes_in_blocks))
            return fromPartialToSpill();
        else
            return OperatorStatus::NEED_INPUT;
    }
    default:
        throw Exception(fmt::format("Unexpected status: {}.", magic_enum::enum_name(status)));
    }
}

ReturnOpStatus MergeSortTransformOp::tryOutputImpl(Block & block)
{
    switch (status)
    {
    case MergeSortStatus::PARTIAL:
        if (sort_spill_context->updateRevocableMemory(sum_bytes_in_blocks))
            return fromPartialToSpill();
        return OperatorStatus::NEED_INPUT;
    case MergeSortStatus::SPILL:
    {
        assert(cached_handler);
        return cached_handler->batchRead() ? OperatorStatus::IO_OUT : fromSpillToPartial();
    }
    case MergeSortStatus::MERGE:
    {
        if likely (merge_impl)
            block = getMergeOutput();
        return OperatorStatus::HAS_OUTPUT;
    }
    case MergeSortStatus::RESTORE:
    {
        if (restored_result.hasData())
        {
            block = restored_result.output();
            return OperatorStatus::HAS_OUTPUT;
        }
        return OperatorStatus::IO_IN;
    }
    default:
        throw Exception(fmt::format("Unexpected status: {}.", magic_enum::enum_name(status)));
    }
}

ReturnOpStatus MergeSortTransformOp::executeIOImpl()
{
    switch (status)
    {
    case MergeSortStatus::SPILL:
    {
        assert(cached_handler);
        cached_handler->spill();
        return OperatorStatus::NEED_INPUT;
    }
    case MergeSortStatus::RESTORE:
    {
        restored_result.put(getMergeOutput());
        return OperatorStatus::HAS_OUTPUT;
    }
    default:
        throw Exception(fmt::format("Unexpected status: {}.", magic_enum::enum_name(status)));
    }
}

void MergeSortTransformOp::transformHeaderImpl(Block &) {}

bool MergeSortTransformOp::RestoredResult::hasData() const
{
    return finished || block.has_value();
}

void MergeSortTransformOp::RestoredResult::put(Block && ret)
{
    assert(!hasData());
    if unlikely (!ret)
        finished = true;
    block.emplace(std::move(ret));
}

Block MergeSortTransformOp::RestoredResult::output()
{
    if unlikely (finished)
        return {};
    Block ret = std::move(*block);
    block.reset();
    return ret;
}

} // namespace DB
