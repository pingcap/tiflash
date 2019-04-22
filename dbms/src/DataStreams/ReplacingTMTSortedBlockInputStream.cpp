#include <DataStreams/ReplacingTMTSortedBlockInputStream.h>
#include <Storages/MutableSupport.h>

namespace DB
{

void ReplacingTMTSortedBlockInputStream::insertRow(MutableColumns & merged_columns, size_t & merged_rows)
{
    ++merged_rows;
    for (size_t i = 0; i < num_columns; ++i)
        merged_columns[i]->insertFrom(*(*selected_row.columns)[i], selected_row.row_num);
}

Block ReplacingTMTSortedBlockInputStream::readImpl()
{
    if (finished)
        return Block();

    MutableColumns merged_columns;
    init(merged_columns);

    if (has_collation)
        throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

    if (merged_columns.empty())
        return Block();

    merge(merged_columns, queue);

    if (deleted_by_range)
    {
        std::ostringstream ss;
        for (size_t i = 0; i < begin_handle_ranges.size(); ++i)
            ss << "(" << begin_handle_ranges[i] << "," << end_handle_ranges[i] << ") ";
        LOG_TRACE(log, "deleted by handle range: " << deleted_by_range << " rows, handle ranges: " << ss.str());
    }

    return header.cloneWithColumns(std::move(merged_columns));
}

// REVIEW: this will be very slow, see ReplacingDeletingSortedBlockInputStream::merge_optimized, it's a good optimizing example
void ReplacingTMTSortedBlockInputStream::merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue)
{
    size_t merged_rows = 0;

    while (!queue.empty())
    {
        SortCursor current = queue.top();

        if (current_key.empty())
        {
            setRowRef(selected_row, current);
            setPrimaryKeyRef(current_key, current);
        }

        setPrimaryKeyRef(next_key, current);

        bool key_differs = (next_key != current_key);

        if (key_differs && merged_rows >= max_block_size)
        {
            logRowGoing("merged_rows >= max_block_size", false);
            return;
        }

        queue.pop();

        if (key_differs)
        {
            if (shouldOutput())
                insertRow(merged_columns, merged_rows);

            selected_row.reset();
            setRowRef(selected_row, current);

            current_key.swap(next_key);
        }
        else
        {
            logRowGoing("!key_differs", false);
        }

        if (!current->isLast())
        {
            current->next();
            queue.push(current);
        }
        else
        {
            fetchNextBlock(current, queue);
        }
    }

    insertRow(merged_columns, merged_rows);

    finished = true;
}

bool ReplacingTMTSortedBlockInputStream::shouldOutput()
{
    if (isDefiniteDeleted())
    {
        ++deleted_by_range;
        logRowGoing("DefiniteDelete", false);
        return false;
    }

    if (nextHasDiffPk())
    {
        logRowGoing("PkLastRow", true);
        return true;
    }

    if (!behindGcTso())
    {
        logRowGoing("KeepHistory", true);
        return true;
    }

    logRowGoing("DiscardHistory", false);
    return false;
}

bool ReplacingTMTSortedBlockInputStream::behindGcTso()
{
    return (*(*selected_row.columns)[version_column_number])[selected_row.row_num].template get<UInt64>() < gc_tso;
}

bool ReplacingTMTSortedBlockInputStream::nextHasDiffPk()
{
    return (*(*selected_row.columns)[pk_column_number])[selected_row.row_num] != (*(*next_key.columns)[0])[next_key.row_num];
}

bool ReplacingTMTSortedBlockInputStream::isDefiniteDeleted()
{
    if (begin_handle_ranges.empty())
        return true;
    HandleID handle_id = (*(*selected_row.columns)[pk_column_number])[selected_row.row_num].template get<HandleID>();
    int pa = std::upper_bound(begin_handle_ranges.begin(), begin_handle_ranges.end(), handle_id) - begin_handle_ranges.begin();
    if (pa == 0)
        return true;
    else
    {
        if (handle_id < end_handle_ranges[pa - 1])
            return false;
        return true;
    }
}

void ReplacingTMTSortedBlockInputStream::logRowGoing(const std::string & msg, bool is_output)
{
    // Disable debug log
    return;

    auto curr_pk = (*(*selected_row.columns)[pk_column_number])[selected_row.row_num].template get<size_t>();
    auto curr_ver = (*(*selected_row.columns)[version_column_number])[selected_row.row_num].template get<size_t>();
    auto curr_del = (*(*selected_row.columns)[del_column_number])[selected_row.row_num].template get<UInt8>();

    auto next_pk = applyVisitor(FieldVisitorToString(), (*(*next_key.columns)[0])[next_key.row_num]);

    LOG_DEBUG(log,
        "gc tso: " << gc_tso << ". "
                   << "curr{pk: " << curr_pk << ", npk: " << next_pk << ", ver: " << curr_ver << ", del: " << size_t(curr_del)
                   << ". same=" << ((toString(curr_pk) == next_pk) ? "true" : "false") << ". why{" << msg << "}, output: " << is_output);
}

} // namespace DB
