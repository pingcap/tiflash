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

#include <DataStreams/MergeSortingBlocksBlockInputStream.h>

namespace DB
{
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

    return !has_collation ? mergeImpl<SortCursor>(queue) : mergeImpl<SortCursorWithCollation>(queue_with_collation);
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
} // namespace DB
