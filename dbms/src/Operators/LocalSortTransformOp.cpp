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
#include <DataStreams/SortHelper.h>
#include <Interpreters/sortBlock.h>
#include <Operators/LocalSortTransformOp.h>

namespace DB
{
void LocalSortTransformOp::operatePrefix()
{
    header_without_constants = getHeader();
    SortHelper::removeConstantsFromBlock(header_without_constants);
    SortHelper::removeConstantsFromSortDescription(header, order_desc);
    // For order by constants, generate LimitOperator instead of SortOperator.
    assert(!order_desc.empty());
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
        SortHelper::removeConstantsFromBlock(block);
        sortBlock(block, order_desc, limit);
        sorted_blocks.emplace_back(std::move(block));
        return OperatorStatus::NEED_INPUT;
    }
    case LocalSortStatus::MERGE:
        throw Exception("Unexpected status: MERGE.");
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
    }
}

void LocalSortTransformOp::transformHeaderImpl(Block &)
{
}

} // namespace DB
