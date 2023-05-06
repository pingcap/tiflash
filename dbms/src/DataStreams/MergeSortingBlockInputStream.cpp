// Copyright 2022 PingCAP, Ltd.
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
#include <IO/CompressedWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <common/logger_useful.h>

namespace DB
{
MergeSortingBlockInputStream::MergeSortingBlockInputStream(
    const BlockInputStreamPtr & input,
    const SortDescription & description_,
    size_t max_merged_block_size_,
    size_t limit_,
    size_t max_bytes_before_external_sort_,
    const SpillConfig & spill_config_,
    const String & req_id)
    : description(description_)
    , max_merged_block_size(max_merged_block_size_)
    , limit(limit_)
    , max_bytes_before_external_sort(max_bytes_before_external_sort_)
    , spill_config(spill_config_)
    , log(Logger::get(req_id))
{
    children.push_back(input);
    header = children.at(0)->getHeader();
    header_without_constants = header;
    SortHelper::removeConstantsFromBlock(header_without_constants);
    SortHelper::removeConstantsFromSortDescription(header, description);
    spiller = std::make_unique<Spiller>(spill_config, true, 1, header_without_constants, log);
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
            if (max_bytes_before_external_sort && sum_bytes_in_blocks > max_bytes_before_external_sort)
            {
                if (!spiller->hasSpilledData())
                {
                    LOG_INFO(log, "Begin spill in sort");
                }
                auto block_in = std::make_shared<MergeSortingBlocksBlockInputStream>(blocks, description, log->identifier(), max_merged_block_size, limit);
                auto is_cancelled_pred = [this]() {
                    return this->isCancelled();
                };
                spiller->spillBlocksUsingBlockInputStream(block_in, 0, is_cancelled_pred);
                blocks.clear();
                if (is_cancelled)
                    break;
                sum_bytes_in_blocks = 0;
            }
        }

        if (isCancelledOrThrowIfKilled() || (blocks.empty() && !spiller->hasSpilledData()))
            return Block();

        if (!spiller->hasSpilledData())
        {
            impl = std::make_unique<MergeSortingBlocksBlockInputStream>(blocks, description, log->identifier(), max_merged_block_size, limit);
        }
        else
        {
            /// If spill happens

            LOG_INFO(log, "Begin restore data from disk for merge sort.");

            /// Create sorted streams to merge.
            spiller->finishSpill();
            inputs_to_merge = spiller->restoreBlocks(0, 0);

            /// Rest of blocks in memory.
            if (!blocks.empty())
                inputs_to_merge.emplace_back(std::make_shared<MergeSortingBlocksBlockInputStream>(
                    blocks,
                    description,
                    log->identifier(),
                    max_merged_block_size,
                    limit));

            /// Will merge that sorted streams.
            impl = std::make_unique<MergingSortedBlockInputStream>(inputs_to_merge, description, max_merged_block_size, limit);
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
