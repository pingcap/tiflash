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

#include <Core/SpillHandler.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/MergeSortingBlocksBlockInputStream.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/SortHelper.h>
#include <DataStreams/copyData.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <common/logger_useful.h>

namespace DB
{
MergeSortingBlockInputStream::MergeSortingBlockInputStream(
    const BlockInputStreamPtr & input,
    const SortDescription & description_,
    size_t max_merged_block_size_,
    size_t limit_,
    size_t max_bytes_before_external_sort,
    const SpillConfig & spill_config,
    const String & req_id,
    const RegisterOperatorSpillContext & register_operator_spill_context)
    : description(description_)
    , max_merged_block_size(max_merged_block_size_)
    , limit(limit_)
    , log(Logger::get(req_id))
{
    children.push_back(input);
    header = children.at(0)->getHeader();
    header_without_constants = header;
    SortHelper::removeConstantsFromBlock(header_without_constants);
    SortHelper::removeConstantsFromSortDescription(header, description);
    sort_spill_context = std::make_shared<SortSpillContext>(spill_config, max_bytes_before_external_sort, log);
    if (register_operator_spill_context != nullptr)
        register_operator_spill_context(sort_spill_context);
    if (sort_spill_context->isSpillEnabled())
        sort_spill_context->buildSpiller(header_without_constants);
}

void MergeSortingBlockInputStream::spillCurrentBlocks()
{
    sort_spill_context->markSpilled();
    auto block_in = std::make_shared<MergeSortingBlocksBlockInputStream>(
        blocks,
        description,
        log->identifier(),
        std::max(1, max_merged_block_size / 10),
        limit);
    auto is_cancelled_pred = [this]() {
        return this->isCancelled();
    };
    sort_spill_context->getSpiller()->spillBlocksUsingBlockInputStream(block_in, 0, is_cancelled_pred);
    sort_spill_context->finishOneSpill();
    blocks.clear();
    sum_bytes_in_blocks = 0;
}

Block MergeSortingBlockInputStream::readImpl()
{
    /** Algorithm:
      * - read to memory blocks from source stream;
      * - if too many of them and if external sorting is enabled,
      *   - merge all blocks to sorted stream and write it to temporary file;
      * - at the end, merge all sorted streams from temporary files and also from rest of blocks in memory.
      */

    /// If has not read source blocks.
    if (!impl)
    {
        while (Block block = children.back()->read())
        {
            /// If there were only const columns in sort description, then there is no need to sort.
            /// Return the blocks as is.
            if (description.empty())
                return block;

            SortHelper::removeConstantsFromBlock(block);

            blocks.push_back(block);
            sum_bytes_in_blocks += block.estimateBytesForSpill();

            /** If too many of them and if external sorting is enabled,
              *  will merge blocks that we have in memory at this moment and write merged stream to temporary (compressed) file.
              * NOTE. It's possible to check free space in filesystem.
              */
            if (sort_spill_context->updateRevocableMemory(sum_bytes_in_blocks))
            {
                spillCurrentBlocks();
                if (is_cancelled)
                    break;
            }
        }

        sort_spill_context->finishSpillableStage();
        if (!blocks.empty() && sort_spill_context->needFinalSpill())
        {
            spillCurrentBlocks();
        }

        if (isCancelledOrThrowIfKilled() || (blocks.empty() && !hasSpilledData()))
            return Block();

        if (!hasSpilledData())
        {
            impl = std::make_unique<MergeSortingBlocksBlockInputStream>(
                blocks,
                description,
                log->identifier(),
                max_merged_block_size,
                limit);
        }
        else
        {
            /// If spill happens

            LOG_INFO(log, "Begin restore data from disk for merge sort.");

            /// Create sorted streams to merge.
            sort_spill_context->getSpiller()->finishSpill();
            inputs_to_merge = sort_spill_context->getSpiller()->restoreBlocks(0, 0);

            /// Rest of blocks in memory.
            if (!blocks.empty())
                inputs_to_merge.emplace_back(std::make_shared<MergeSortingBlocksBlockInputStream>(
                    blocks,
                    description,
                    log->identifier(),
                    max_merged_block_size,
                    limit));

            /// Will merge that sorted streams.
            impl = std::make_unique<MergingSortedBlockInputStream>(
                inputs_to_merge,
                description,
                max_merged_block_size,
                limit);
        }
    }

    Block res = impl->read();
    if (res)
        SortHelper::enrichBlockWithConstants(res, header);
    return res;
}

void MergeSortingBlockInputStream::appendInfo(FmtBuffer & buffer) const
{
    buffer.fmtAppend(", limit = {}", limit);
}
} // namespace DB
