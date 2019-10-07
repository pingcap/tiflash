#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/TMTSortedBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template <TMTPKType pk_type>
void TMTSortedBlockInputStream<pk_type>::insertRow(MutableColumns & merged_columns, size_t & merged_rows)
{
    ++merged_rows;
    for (size_t i = 0; i < num_columns; ++i)
        merged_columns[i]->insertFrom(*(*selected_row.columns)[i], selected_row.row_num);
}

template <TMTPKType pk_type>
Block TMTSortedBlockInputStream<pk_type>::readImpl()
{
    if (finished)
    {
        LOG_TRACE(log,
            "read by_row:" + toString(by_row) + ", by_column: " + toString(by_column) + ", "
                + toString(((Float64)by_column) / (by_row + by_column) * 100, 2) + "%");
        return Block();
    }

    MutableColumns merged_columns;
    init(merged_columns);

    if (has_collation)
        throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

    if (merged_columns.empty())
        return Block();

    mergeOptimized(merged_columns, tmt_queue);

    auto res = header.cloneWithColumns(std::move(merged_columns));
    return res;
}

template <TMTPKType pk_type>
void TMTSortedBlockInputStream<pk_type>::mergeOptimized(MutableColumns & merged_columns, std::priority_queue<TMTSortCursorPK> & queue)
{
    size_t merged_rows = 0;

    /// Take the rows in needed order and put them into `merged_columns` until rows no more than `max_block_size`
    while (!cur_block_cursor.none() || !queue.empty())
    {
        TMTSortCursorPK current;
        bool is_complete_top;

        if (cur_block_cursor.none())
        {
            const TMTSortCursorPK top = queue.top();
            queue.pop();

            if (top->empty())
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

                current = TMTSortCursorPK(&cur_block_cursor_impl);
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
                cur_block_cursor = TMTSortCursorPK();
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
                if (current.notSame(cur_block_cursor))
                    queue.push(current);
                return;
            }

            if (current_key.empty())
                setPrimaryKeyRefOptimized(current_key, current);

            setPrimaryKeyRefOptimized(next_key, current);

            const auto key_differs
                = cmpTMTCursor<true, true, pk_type>(*current_key.columns, current_key.row_num, *next_key.columns, next_key.row_num);

            if (key_differs.all)
            {
                by_row++;

                max_version = 0;
                /// Write the data for the previous primary key.
                if (!max_delmark)
                    insertRow(merged_columns, merged_rows);

                if (is_clean_top)
                {
                    /// Delete current cache and return.
                    /// We will come back later and use current block's data directly.
                    max_delmark = (UInt8)1;
                    current_key.reset();
                    selected_row.reset();

                    if (current.notSame(cur_block_cursor))
                        queue.push(current);
                    return;
                }
                else
                {
                    max_delmark = 0;
                    current_key.swap(next_key);
                }
            }

            UInt64 version = static_cast<const ColumnUInt64 *>(current->all_columns[version_column_index])->getElement(current->pos);
            UInt8 delmark = static_cast<const ColumnUInt8 *>(current->all_columns[delmark_column_index])->getElement(current->pos);

            /// A non-strict comparison, since we select the last row for the same version values.
            if ((version > max_version) || (version == max_version && delmark >= max_delmark))
            {
                max_version = version;
                max_delmark = delmark;
                setRowRefOptimized(selected_row, current);
            }

            if (current->isLast())
            {
                /// We get the next block from the corresponding source, if there is one.
                if (current.notSame(cur_block_cursor))
                {
                    fetchNextBlock(current, queue);
                }
                else
                {
                    cur_block_cursor = TMTSortCursorPK();
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
                    if (current.notSame(cur_block_cursor))
                        queue.push(current);
                    else
                        throw Exception(
                            "[TMTSortedBlockInputStream::merge_optimized] current == cur_block_cursor", ErrorCodes::LOGICAL_ERROR);
                    break; /// Break current block loop.
                }
            }
        }
    }

    /// We will write the data for the last primary key.
    if (!max_delmark && !current_key.empty())
        insertRow(merged_columns, merged_rows);

    if (cur_block)
    {
        /// Clear cache.
        cur_block_cursor_impl = SortCursorImpl();
        cur_block.reset();
    }

    finished = true;
}

template <TMTPKType pk_type>
bool TMTSortedBlockInputStream<pk_type>::insertByColumn(TMTSortCursorPK current, size_t & merged_rows, MutableColumns & merged_columns)
{
    if (current.notSame(cur_block_cursor))
        throw Exception("[TMTSortedBlockInputStream::insertByColumn] current != cur_block_cursor", ErrorCodes::LOGICAL_ERROR);

    bool give_up = false;
    RowRef cur_key;
    for (size_t i = 0, size = current->all_columns[0]->size(); i < size; i++)
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

            const auto key_differs = cmpTMTCursor<true, true, pk_type>(*cur_key.columns, cur_key.row_num, *key.columns, key.row_num);

            if (!key_differs.all)
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

    bool direct_move = true;

    {
        const auto del_column = typeid_cast<const ColumnUInt8 *>(current->all_columns[delmark_column_index]);

        // reverse_filter - 1: delete, 0: remain.
        // filter         - 0: delete, 1: remain.
        const IColumn::Filter & reverse_filter = del_column->getData();
        direct_move = memoryIsZero(reverse_filter.data(), reverse_filter.size());

        if (!direct_move)
        {
            IColumn::Filter filter(reverse_filter.size());
            for (size_t i = 0; i < reverse_filter.size(); i++)
                filter[i] = reverse_filter[i] ^ (UInt8)1;

            for (size_t i = 0; i < num_columns; ++i)
            {
                ColumnPtr column = cur_block->getByPosition(i).column->filter(filter, -1);
                merged_columns[i] = (*std::move(column)).mutate();
            }
        }
    }

    if (direct_move)
    {
        for (size_t i = 0; i < num_columns; ++i)
        {
            ColumnPtr column = cur_block->getByPosition(i).column;
            merged_columns[i] = (*std::move(column)).mutate();
        }
    }

    merged_rows = merged_columns[0]->size();
    by_column += merged_rows;

    return true;
}

template <TMTPKType pk_type>
void TMTSortedBlockInputStream<pk_type>::initQueue()
{
    for (size_t i = 0; i < cursors.size(); ++i)
        if (!cursors[i].empty())
            tmt_queue.push(TMTSortCursorPK(&cursors[i]));
}

template class TMTSortedBlockInputStream<TMTPKType::UNSPECIFIED>;
template class TMTSortedBlockInputStream<TMTPKType::UINT64>;
template class TMTSortedBlockInputStream<TMTPKType::INT64>;

} // namespace DB
