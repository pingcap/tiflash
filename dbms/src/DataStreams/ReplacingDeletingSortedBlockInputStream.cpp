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

#include <Columns/ColumnsNumber.h>
#include <DataStreams/ReplacingDeletingSortedBlockInputStream.h>
#include <Storages/MutableSupport.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


void ReplacingDeletingSortedBlockInputStream::insertRow(MutableColumns & merged_columns, size_t & merged_rows)
{
    if (out_row_sources_buf)
    {
        /// true flag value means "skip row"
        current_row_sources.back().setSkipFlag(false);

        out_row_sources_buf->write(reinterpret_cast<const char *>(current_row_sources.data()),
                                   current_row_sources.size() * sizeof(RowSourcePart));
        current_row_sources.resize(0);
    }

    ++merged_rows;
    for (size_t i = 0; i < num_columns; ++i)
        merged_columns[i]->insertFrom(*(*selected_row.columns)[i], selected_row.row_num);
}


Block ReplacingDeletingSortedBlockInputStream::readImpl()
{
    if (finished)
    {
        if (is_optimized)
            LOG_FMT_TRACE(log, "read by_row: {}, by_column: {}, {:.2f}%", by_row, by_column, ((Float64)by_column) / (by_row + by_column) * 100);
        return Block();
    }

    MutableColumns merged_columns;
    init(merged_columns);

    if (has_collation)
        throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

    if (merged_columns.empty())
        return Block();

    /// Additional initialization.
    if (selected_row.empty())
    {
        if (!version_column.empty())
            version_column_number = header.getPositionByName(version_column);
        if (!delmark_column.empty())
            delmark_column_number = header.getPositionByName(delmark_column);
    }

    if (is_optimized)
    {
        merge_optimized(merged_columns, queue);
    }
    else
    {
        merge(merged_columns, queue);
    }

    auto res = header.cloneWithColumns(std::move(merged_columns));
    return res;
}


void ReplacingDeletingSortedBlockInputStream::merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue)
{
    size_t merged_rows = 0;

    /// Take the rows in needed order and put them into `merged_columns` until rows no more than `max_block_size`
    while (!queue.empty())
    {
        SortCursor current = queue.top();

        if (current_key.empty())
            setPrimaryKeyRef(current_key, current);

        UInt64 version = version_column_number != -1
            ? current->all_columns[version_column_number]->get64(current->pos)
            : 0;
        UInt64 delmark = delmark_column_number != -1
            ? current->all_columns[delmark_column_number]->get64(current->pos)
            : 0;

        setPrimaryKeyRef(next_key, current);

        bool key_differs = next_key != current_key;

        /// if there are enough rows and the last one is calculated completely
        if (key_differs && merged_rows >= max_block_size)
            return;

        queue.pop();

        if (key_differs)
        {
            max_version = 0;
            /// Write the data for the previous primary key.
            if (!MutableSupport::DelMark::isDel(UInt8(max_delmark)))
                insertRow(merged_columns, merged_rows);
            max_delmark = 0;
            current_key.swap(next_key);
        }

        /// Initially, skip all rows. Unskip last on insert.
        if (out_row_sources_buf)
            current_row_sources.emplace_back(current.impl->order, true);

        /// A non-strict comparison, since we select the last row for the same version values.
        if (version >= max_version)
        {
            max_version = version;
            max_delmark = delmark;
            setRowRef(selected_row, current);
        }

        if (!current->isLast())
        {
            current->next();
            queue.push(current);
        }
        else
        {
            /// We get the next block from the corresponding source, if there is one.
            fetchNextBlock(current, queue);
        }
    }

    /// We will write the data for the last primary key.
    if (!MutableSupport::DelMark::isDel(UInt8(max_delmark)))
        insertRow(merged_columns, merged_rows);

    finished = true;
}

void ReplacingDeletingSortedBlockInputStream::merge_optimized(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue)
{
    size_t merged_rows = 0;

    /// Take the rows in needed order and put them into `merged_columns` until rows no more than `max_block_size`
    while (!cur_block_cursor.none() || !queue.empty())
    {
        SortCursor current;
        bool is_complete_top;

        if (cur_block_cursor.none())
        {
            const SortCursor top = queue.top();
            queue.pop();

            if (top.impl->empty())
            {
                fetchNextBlock(top, queue);
                continue;
            }

            is_complete_top = queue.empty() || top.totallyLessOrEquals(queue.top());
            if (is_complete_top)
            {
                /// If all rows of this top block are smaller than others, we cache top block here.
                /// Then pull next block of the same stream slot and put it into queue, so that we can compare (in insertByColumn)
                /// this top block with all others, including the one pulled from the same stream.

                cur_block_cursor_impl = *(top.impl);
                cur_block = source_blocks[top.impl->order];

                fetchNextBlock(top, queue);

                current = SortCursor(&cur_block_cursor_impl);
                cur_block_cursor = current;
            }
            else
            {
                current = top;
            }
        }
        else
        {
            current = cur_block_cursor;
            is_complete_top = true;
        }

        bool is_clean_top = is_complete_top && current->isFirst() && (queue.empty() || current.totallyLessIgnOrder(queue.top()));
        if (is_clean_top && merged_rows == 0 && current_key.empty())
        {
            bool by_column_ok = insertByColumn(current, merged_rows, merged_columns);
            if (by_column_ok)
            {
                cur_block_cursor = SortCursor();
                cur_block_cursor_impl = SortCursorImpl();
                cur_block.reset();
                continue;
            }
        }

        while (true)
        {
            /// If there are enough rows and the last one is calculated completely.
            if (merged_rows >= max_block_size)
            {
                if (current != cur_block_cursor)
                    queue.push(current);
                return;
            }

            if (current_key.empty())
                setPrimaryKeyRefOptimized(current_key, current);

            setPrimaryKeyRefOptimized(next_key, current);

            if (next_key != current_key)
            {
                by_row++;

                max_version = 0;
                /// Write the data for the previous primary key.
                if (!MutableSupport::DelMark::isDel(UInt8(max_delmark)))
                    insertRow(merged_columns, merged_rows);

                if (is_clean_top)
                {
                    /// Delete current cache and return.
                    /// We will come back later and use current block's data directly.
                    max_delmark = MutableSupport::DelMark::genDelMark(true);
                    current_key.reset();
                    selected_row.reset();
                    current_row_sources.resize(0);

                    if (current != cur_block_cursor)
                        queue.push(current);
                    return;
                }
                else
                {
                    max_delmark = 0;
                    current_key.swap(next_key);
                }
            }

            /// Initially, skip all rows. Unskip last on insert.
            if (out_row_sources_buf)
                current_row_sources.emplace_back(current.impl->order, true);

            UInt64 version = version_column_number != -1
                ? current->all_columns[version_column_number]->get64(current->pos)
                : 0;
            UInt64 delmark = delmark_column_number != -1
                ? current->all_columns[delmark_column_number]->get64(current->pos)
                : 0;

            /// A non-strict comparison, since we select the last row for the same version values.
            if (version >= max_version)
            {
                max_version = version;
                max_delmark = delmark;
                setRowRefOptimized(selected_row, current);
            }

            if (current->isLast())
            {
                /// We get the next block from the corresponding source, if there is one.
                if (current != cur_block_cursor)
                {
                    fetchNextBlock(current, queue);
                }
                else
                {
                    cur_block_cursor = SortCursor();
                    // cur_block_cursor_impl = SortCursorImpl();
                    // cur_block.reset();

                    /// No need to fetchNextBlock here as we already do it before.
                }
                break; /// Break current block loop.
            }
            else
            {
                current->next();
                if (is_complete_top || queue.empty() || !(current.greater(queue.top())))
                {
                    continue; /// Continue current block loop.
                }
                else
                {
                    if (current != cur_block_cursor)
                        queue.push(current);
                    else
                        throw Exception("Impossible!");
                    break; /// Break current block loop.
                }
            }
        }
    }

    /// We will write the data for the last primary key.
    if (!MutableSupport::DelMark::isDel(UInt8(max_delmark)) && !current_key.empty())
        insertRow(merged_columns, merged_rows);

    if (cur_block)
    {
        /// Clear cache.
        cur_block_cursor_impl = SortCursorImpl();
        cur_block.reset();
    }

    finished = true;
}

// TODO: use MutableSupport::DelMark here to check and generate del-mark
bool ReplacingDeletingSortedBlockInputStream::insertByColumn(SortCursor current,
                                                             size_t & merged_rows,
                                                             MutableColumns & merged_columns)
{
    if (current != cur_block_cursor)
        throw Exception("Logical error!");

    bool give_up = false;
    RowRef cur_key;
    for (size_t i = 0; i < current->all_columns[0]->size(); i++)
    {
        /// If we find any continually equal keys, give up by_column optimization.
        if (cur_key.empty())
        {
            setPrimaryKeyRefOptimized(cur_key, current);
        }
        else
        {
            RowRef key;
            setPrimaryKeyRefOptimized(key, current);
            if (cur_key == key)
            {
                give_up = true;
                break;
            }
            cur_key.swap(key);
        }
        current->next();
    }
    // Reset to zero, as other code may use it later.
    current.impl->pos = 0;

    if (give_up)
    {
        return false;
    }

    size_t source_num = current.impl->order;

    bool direct_move = true;
    if (version_column_number != -1)
    {
        const auto * del_column = typeid_cast<const ColumnUInt8 *>(current->all_columns[delmark_column_number]);

        // reverse_filter - 1: delete, 0: remain.
        // filter         - 0: delete, 1: remain.
        const IColumn::Filter & reverse_filter = del_column->getData();
        IColumn::Filter filter(reverse_filter.size());
        bool no_delete = true;

        for (size_t i = 0; i < reverse_filter.size(); i++)
        {
            no_delete &= !reverse_filter[i];
            filter[i] = reverse_filter[i] ^ static_cast<UInt8>(1);
        }

        direct_move = no_delete;
        if (!direct_move)
        {
            for (size_t i = 0; i < num_columns; ++i)
            {
                ColumnPtr column = cur_block->getByPosition(i).column->filter(filter, -1);
                merged_columns[i] = (*std::move(column)).mutate();
            }

            RowSourcePart row_source(source_num);
            current_row_sources.resize(filter.size());
            for (size_t i = 0; i < filter.size(); ++i)
            {
                row_source.setSkipFlag(reverse_filter[i]);
                current_row_sources[i] = row_source.data;
            }
            if (out_row_sources_buf)
                out_row_sources_buf->write(reinterpret_cast<const char *>(current_row_sources.data()),
                                           current_row_sources.size());
            current_row_sources.resize(0);
        }
    }

    if (direct_move)
    {
        for (size_t i = 0; i < num_columns; ++i)
        {
            ColumnPtr column = cur_block->getByPosition(i).column;
            merged_columns[i] = (*std::move(column)).mutate();
        }

        if (out_row_sources_buf)
        {
            for (size_t i = 0; i < merged_rows; ++i)
            {
                RowSourcePart row_source(source_num);
                out_row_sources_buf->write(row_source.data);
            }
        }
    }

    merged_rows = merged_columns[0]->size();
    by_column += merged_rows;

    return true;
}

} // namespace DB
