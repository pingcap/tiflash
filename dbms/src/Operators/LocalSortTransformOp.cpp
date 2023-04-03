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

#include <DataStreams/MergeSortingBlocksBlockInputStream.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <DataStreams/SortHelper.h>
#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Interpreters/sortBlock.h>
#include <Operators/LocalSortTransformOp.h>

#include <magic_enum.hpp>

namespace DB
{
void LocalSortTransformOp::operatePrefix()
{
    header_without_constants = getHeader();
    SortHelper::removeConstantsFromBlock(header_without_constants);
    SortHelper::removeConstantsFromSortDescription(header, order_desc);
    // For order by constants, generate LimitOperator instead of SortOperator.
    assert(!order_desc.empty());

    spiller = std::make_unique<Spiller>(spill_config, true, 1, header_without_constants, log);
}

void LocalSortTransformOp::operateSuffix()
{
    if likely (merge_impl)
        merge_impl->readSuffix();
}

Block LocalSortTransformOp::getMergeOutput()
{
    assert(merge_impl);
    Block block = merge_impl->read();
    if likely (block)
        SortHelper::enrichBlockWithConstants(block, header);
    return block;
}

OperatorStatus LocalSortTransformOp::transformImpl(Block & block)
{
    switch (status)
    {
    case LocalSortStatus::PARTIAL:
    {
        if unlikely (!block)
        {
            if (!spiller->hasSpilledData())
            {
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
            else
            {
                // convert to merge phase.
                status = LocalSortStatus::RESTORE;

                /// If spill happens
                LOG_INFO(log, "Begin restore data from disk for merge sort.");
    
                /// Create sorted streams to merge.
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
    
                /// Will merge that sorted streams.
                merge_impl = std::make_unique<MergingSortedBlockInputStream>(inputs_to_merge, order_desc, max_block_size, limit);
                merge_impl->readPrefix();
                return OperatorStatus::IO;
            }
        }
        SortHelper::removeConstantsFromBlock(block);
        sortBlock(block, order_desc, limit);
        sum_bytes_in_blocks += block.bytes();
        sorted_blocks.emplace_back(std::move(block));
        if (max_bytes_before_external_sort && sum_bytes_in_blocks > max_bytes_before_external_sort)
        {
            // spill
            return OperatorStatus::IO;
        }
        return OperatorStatus::NEED_INPUT;
    }
    default:
        throw Exception(fmt::format("Unexpected status: {}.", magic_enum::enum_name(status)));
    }
}

OperatorStatus LocalSortTransformOp::tryOutputImpl(Block & block)
{
    switch (status)
    {
    case LocalSortStatus::PARTIAL:
        return OperatorStatus::NEED_INPUT;
    case LocalSortStatus::MERGE:
    {
        if likely (merge_impl)
            block = getMergeOutput();
        return OperatorStatus::HAS_OUTPUT;
    }
    case LocalSortStatus::RESTORE:
    {
        if (!restore_result)
            return OperatorStatus::IO;
        block = std::move(*restore_result);
        restore_result.reset();
        return OperatorStatus::HAS_OUTPUT;
    }
    default:
        throw Exception(fmt::format("Unexpected status: {}.", magic_enum::enum_name(status)));
    }
}

OperatorStatus LocalSortTransformOp::executeIOImpl()
{
    switch (status)
    {
    case LocalSortStatus::PARTIAL:
    {
        MergeSortingBlocksBlockInputStream block_in(sorted_blocks, order_desc, log->identifier(), max_block_size, limit);
        spiller->spillBlocksUsingBlockInputStream(block_in, 0, [&]() { return exec_status.isCancelled(); });
        sorted_blocks.clear();
        sum_bytes_in_blocks = 0;
        return OperatorStatus::NEED_INPUT;
    }
    case LocalSortStatus::RESTORE:
    {
        assert(!restore_result);
        restore_result.emplace(getMergeOutput());
        return OperatorStatus::HAS_OUTPUT;
    }
    default:
        throw Exception(fmt::format("Unexpected status: {}.", magic_enum::enum_name(status)));
    }
}

void LocalSortTransformOp::transformHeaderImpl(Block &)
{
}

} // namespace DB
