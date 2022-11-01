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

#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>

namespace DB
{
/** Remove constant columns from block.
  */
static void removeConstantsFromBlock(Block & block)
{
    size_t columns = block.columns();
    size_t i = 0;
    while (i < columns)
    {
        if (block.getByPosition(i).column->isColumnConst())
        {
            block.erase(i);
            --columns;
        }
        else
            ++i;
    }
}

static void removeConstantsFromSortDescription(const Block & header, SortDescription & description)
{
    description.erase(
        std::remove_if(description.begin(), description.end(), [&](const SortColumnDescription & elem) {
            if (!elem.column_name.empty())
                return header.getByName(elem.column_name).column->isColumnConst();
            else
                return header.safeGetByPosition(elem.column_number).column->isColumnConst();
        }),
        description.end());
}

/** Add into block, whose constant columns was removed by previous function,
  *  constant columns from header (which must have structure as before removal of constants from block).
  */
static void enrichBlockWithConstants(Block & block, const Block & header)
{
    size_t rows = block.rows();
    size_t columns = header.columns();

    for (size_t i = 0; i < columns; ++i)
    {
        const auto & col_type_name = header.getByPosition(i);
        if (col_type_name.column->isColumnConst())
            block.insert(i, {col_type_name.column->cloneResized(rows), col_type_name.type, col_type_name.name});
    }
}


MergeSortingBlockInputStream::MergeSortingBlockInputStream(
    const BlockInputStreamPtr & input,
    const SortDescription & description_,
    size_t max_merged_block_size_,
    size_t limit_,
    size_t max_bytes_before_external_sort_,
    const std::string & tmp_path_,
    const String & req_id)
    : description(description_)
    , max_merged_block_size(max_merged_block_size_)
    , limit(limit_)
    , max_bytes_before_external_sort(max_bytes_before_external_sort_)
    , tmp_path(tmp_path_)
    , log(Logger::get(req_id))
{
    children.push_back(input);
    header = children.at(0)->getHeader();
    header_without_constants = header;
    removeConstantsFromBlock(header_without_constants);
    removeConstantsFromSortDescription(header, description);
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

            removeConstantsFromBlock(block);

            blocks.push_back(block);
            sum_bytes_in_blocks += block.bytes();

            /** If too many of them and if external sorting is enabled,
              *  will merge blocks that we have in memory at this moment and write merged stream to temporary (compressed) file.
              * NOTE. It's possible to check free space in filesystem.
              */
            if (max_bytes_before_external_sort && sum_bytes_in_blocks > max_bytes_before_external_sort)
            {
                temporary_files.emplace_back(new Poco::TemporaryFile(tmp_path));
                const std::string & path = temporary_files.back()->path();
                WriteBufferFromFile file_buf(path);
                CompressedWriteBuffer compressed_buf(file_buf);
                NativeBlockOutputStream block_out(compressed_buf, 0, header_without_constants);
                MergeSortingBlocksBlockInputStream block_in(blocks, description, log->identifier(), max_merged_block_size, limit);

                LOG_INFO(log, "Sorting and writing part of data into temporary file {}", path);
                copyData(block_in, block_out, &is_cancelled); /// NOTE. Possibly limit disk usage.
                LOG_INFO(log, "Done writing part of data into temporary file {}", path);

                blocks.clear();
                sum_bytes_in_blocks = 0;
            }
        }

        if ((blocks.empty() && temporary_files.empty()) || isCancelledOrThrowIfKilled())
            return Block();

        if (temporary_files.empty())
        {
            impl = std::make_unique<MergeSortingBlocksBlockInputStream>(blocks, description, log->identifier(), max_merged_block_size, limit);
        }
        else
        {
            /// If there was temporary files.

            LOG_INFO(log, "There are {} temporary sorted parts to merge.", temporary_files.size());

            /// Create sorted streams to merge.
            for (const auto & file : temporary_files)
            {
                temporary_inputs.emplace_back(std::make_unique<TemporaryFileStream>(file->path(), header_without_constants));
                inputs_to_merge.emplace_back(temporary_inputs.back()->block_in);
            }

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
        enrichBlockWithConstants(res, header);
    return res;
}


MergeSortingBlocksBlockInputStream::MergeSortingBlocksBlockInputStream(
    Blocks & blocks_,
    SortDescription & description_,
    const String & req_id,
    size_t max_merged_block_size_,
    size_t limit_)
    : blocks(blocks_)
    , header(blocks.at(0).cloneEmpty())
    , description(description_)
    , max_merged_block_size(max_merged_block_size_)
    , limit(limit_)
    , log(Logger::get(req_id))
{
    Blocks nonempty_blocks;
    for (const auto & block : blocks)
    {
        if (block.rows() == 0)
            continue;

        nonempty_blocks.push_back(block);
        cursors.emplace_back(block, description);
        has_collation |= cursors.back().has_collation;
    }

    blocks.swap(nonempty_blocks);

    if (!has_collation)
    {
        for (auto & cursor : cursors)
            queue.push(SortCursor(&cursor));
    }
    else
    {
        for (auto & cursor : cursors)
            queue_with_collation.push(SortCursorWithCollation(&cursor));
    }
}


Block MergeSortingBlocksBlockInputStream::readImpl()
{
    if (blocks.empty())
        return Block();

    if (blocks.size() == 1)
    {
        Block res = blocks[0];
        blocks.clear();
        return res;
    }

    return !has_collation
        ? mergeImpl<SortCursor>(queue)
        : mergeImpl<SortCursorWithCollation>(queue_with_collation);
}


template <typename TSortCursor>
Block MergeSortingBlocksBlockInputStream::mergeImpl(std::priority_queue<TSortCursor> & queue)
{
    size_t num_columns = blocks[0].columns();

    MutableColumns merged_columns = blocks[0].cloneEmptyColumns();
    /// TODO: reserve (in each column)

    /// Take rows from queue in right order and push to 'merged'.
    size_t merged_rows = 0;
    while (!queue.empty())
    {
        TSortCursor current = queue.top();
        queue.pop();

        for (size_t i = 0; i < num_columns; ++i)
            merged_columns[i]->insertFrom(*current->all_columns[i], current->pos);

        if (!current->isLast())
        {
            current->next();
            queue.push(current);
        }

        ++total_merged_rows;
        if (limit && total_merged_rows == limit)
        {
            auto res = blocks[0].cloneWithColumns(std::move(merged_columns));
            blocks.clear();
            return res;
        }

        ++merged_rows;
        if (merged_rows == max_merged_block_size)
            return blocks[0].cloneWithColumns(std::move(merged_columns));
    }

    if (merged_rows == 0)
        return {};

    return blocks[0].cloneWithColumns(std::move(merged_columns));
}

void MergeSortingBlockInputStream::appendInfo(FmtBuffer & buffer) const
{
    buffer.fmtAppend(", limit = {}", limit);
}

} // namespace DB
