#include <Columns/ColumnsNumber.h>
#include <DataStreams/ReplacingTMTSortedBlockInputStream.h>
#include <Storages/MutableSupport.h>

namespace DB
{

constexpr ssize_t PK_ORDER_DESC_POS = 0;
constexpr ssize_t VERSION_ORDER_DESC_POS = 1;
constexpr ssize_t DELMARK_ORDER_DESC_POS = 2;

template <typename HandleType>
void ReplacingTMTSortedBlockInputStream<HandleType>::insertRow(MutableColumns & merged_columns, size_t & merged_rows)
{
    ++merged_rows;
    for (size_t i = 0; i < num_columns; ++i)
        merged_columns[i]->insertFrom(*(*selected_row.columns)[i], selected_row.row_num);
}

template <typename HandleType>
Block ReplacingTMTSortedBlockInputStream<HandleType>::readImpl()
{
    if (finished)
    {
        LOG_DEBUG(log,
            __FUNCTION__ << ": table " << table_id << ", gc safe point " << gc_tso << ", diff pk " << diff_pk << ", final del " << final_del
                         << ", keep history " << keep_history << ", dis history " << dis_history);

        if (deleted_by_range)
        {
            std::stringstream ss;

            if (log->trace())
            {
                for (size_t i = 0; i < begin_handle_ranges.size(); ++i)
                {
                    ss << "[";
                    begin_handle_ranges[i].toString(ss);
                    ss << ",";
                    end_handle_ranges[i].toString(ss);
                    ss << ") ";
                }
            }

            LOG_DEBUG(log,
                __FUNCTION__ << ": deleted by handle range " << deleted_by_range << " rows, " << begin_handle_ranges.size()
                             << " handle ranges " << ss.str());
        }
    }

    if (finished)
        return Block();

    MutableColumns merged_columns;
    init(merged_columns);

    if (merged_columns.empty())
        return Block();

    merge(merged_columns);

    return header.cloneWithColumns(std::move(merged_columns));
}

template <typename HandleType>
void ReplacingTMTSortedBlockInputStream<HandleType>::merge(MutableColumns & merged_columns)
{
    size_t merged_rows = 0;

    while (!tmt_queue.empty())
    {
        TMTSortCursorFull current = tmt_queue.top();

        if (selected_row.empty())
        {
            setRowRef(selected_row, current);
            setPrimaryKeyRef(current_key, current);
        }

        setPrimaryKeyRef(next_key, current);

        const auto key_differs = cmpTMTCursor<true, false, TMTPKType::UNSPECIFIED>(
            *current_key.columns, current_key.row_num, *next_key.columns, next_key.row_num);

        if ((key_differs.diffs[PK_ORDER_DESC_POS] | key_differs.diffs[VERSION_ORDER_DESC_POS]) == 0) // handle and tso are equal.
        {
            tmt_queue.pop();

            if (key_differs.diffs[DELMARK_ORDER_DESC_POS] == 0) // del is equal
            {}
            else
            {
                setRowRef(selected_row, current);
                current_key.swap(next_key);
            }
        }
        else
        {
            if (merged_rows >= max_block_size)
            {
                return;
            }
            else
            {
                tmt_queue.pop();

                if (shouldOutput(key_differs))
                    insertRow(merged_columns, merged_rows);

                setRowRef(selected_row, current);
                current_key.swap(next_key);
            }
        }

        if (!current->isLast())
        {
            current->next();
            tmt_queue.push(current);
        }
        else
        {
            fetchNextBlock(current, tmt_queue);
        }
    }

    if (!selected_row.empty())
    {
        if (isDefiniteDeleted())
            ++deleted_by_range;
        else if (!(final && hasDeleteFlag() && behindGcTso()))
        {
            insertRow(merged_columns, merged_rows);
            ++diff_pk;
        }
        else
            ++final_del;
    }

    finished = true;
}

template <typename HandleType>
bool ReplacingTMTSortedBlockInputStream<HandleType>::shouldOutput(const TMTCmpOptimizedRes res)
{
    if (isDefiniteDeleted())
    {
        ++deleted_by_range;
        return false;
    }

    // next has diff pk
    if (res.diffs[PK_ORDER_DESC_POS])
    {
        if (final && hasDeleteFlag() && behindGcTso())
        {
            ++final_del;
            return false;
        }

        ++diff_pk;

        return true;
    }

    if (behindGcTso())
    {
        // keep the last one lt or eq than gc_tso, or when read_tso is gc_tso, nothing can be read.
        auto next_key_tso = static_cast<const ColumnUInt64 *>((*next_key.columns)[VERSION_ORDER_DESC_POS])->getElement(next_key.row_num);
        if (next_key_tso <= gc_tso)
        {
            ++dis_history;
            return false;
        }
    }
    ++keep_history;
    return true;
}

template <typename HandleType>
bool ReplacingTMTSortedBlockInputStream<HandleType>::behindGcTso()
{
    const ColumnUInt64 * column = static_cast<const ColumnUInt64 *>((*selected_row.columns)[version_column_number]);
    return column->getElement(selected_row.row_num) < gc_tso;
}

template <typename HandleType>
bool ReplacingTMTSortedBlockInputStream<HandleType>::isDefiniteDeleted()
{
    if (begin_handle_ranges.empty())
        return true;
    Handle pk_handle = static_cast<HandleType>((*(*selected_row.columns)[pk_column_number]).getUInt(selected_row.row_num));
    int pa = std::upper_bound(begin_handle_ranges.begin(), begin_handle_ranges.end(), pk_handle) - begin_handle_ranges.begin();
    if (pa == 0)
        return true;
    else
    {
        if (pk_handle < end_handle_ranges[pa - 1])
            return false;
        return true;
    }
}

template <typename HandleType>
bool ReplacingTMTSortedBlockInputStream<HandleType>::hasDeleteFlag()
{
    const ColumnUInt8 * column = static_cast<const ColumnUInt8 *>((*selected_row.columns)[del_column_number]);
    return MutableSupport::DelMark::isDel(column->getElement(selected_row.row_num));
}

template <typename HandleType>
void ReplacingTMTSortedBlockInputStream<HandleType>::initQueue()
{
    for (size_t i = 0; i < cursors.size(); ++i)
        if (!cursors[i].empty())
            tmt_queue.push(TMTSortCursorFull(&cursors[i]));
}

template class ReplacingTMTSortedBlockInputStream<Int64>;
template class ReplacingTMTSortedBlockInputStream<UInt64>;

} // namespace DB
