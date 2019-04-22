#include <DataStreams/MvccTMTSortedBlockInputStream.h>
#include <Storages/MutableSupport.h>

namespace DB
{

void MvccTMTSortedBlockInputStream::insertRow(MutableColumns & merged_columns, size_t & merged_rows)
{
    ++merged_rows;
    for (size_t i = 0; i < num_columns; ++i)
        merged_columns[i] -> insertFrom(*(*selected_row.columns)[i], selected_row.row_num);
}

Block MvccTMTSortedBlockInputStream::readImpl()
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
    return header.cloneWithColumns(std::move(merged_columns));
}

// REVIEW: this will be very slow, see ReplacingDeletingSortedBlockInputStream::merge_optimized, it's a good optimizing example
void MvccTMTSortedBlockInputStream::merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue)
{
    size_t merged_rows = 0;

    while(!queue.empty())
    {
        SortCursor current = queue.top();

        if (current_key.empty())
            setPrimaryKeyRef(current_key, current);

        setPrimaryKeyRef(next_key, current);

        bool key_differs = next_key != current_key;

        if (key_differs && merged_rows >= max_block_size)
            return;

        queue.pop();

        if(key_differs)
        {
            if (!selected_row.empty() && !hasDeleteFlag())
                insertRow(merged_columns, merged_rows);

            selected_row.reset();

            current_key.swap(next_key);
        }

        if ((*(current->all_columns[version_column_number]))[current->pos].template get<UInt64>() <= read_tso && 
            (selected_row.empty() 
            || current->all_columns[version_column_number]->compareAt(
                current->pos, selected_row.row_num, 
                *(*selected_row.columns)[version_column_number],
                1 ) > 0))
        {
            setRowRef(selected_row, current);
        }

        if (!current->isLast()) {
            current->next();
            queue.push(current);
        }
        else
        {
            fetchNextBlock(current, queue);
        }

    }

    if (!selected_row.empty() && !hasDeleteFlag())
        insertRow(merged_columns, merged_rows);

    finished = true;
}

bool MvccTMTSortedBlockInputStream::hasDeleteFlag()
{
    UInt8 val = (*(*selected_row.columns)[del_column_number])[selected_row.row_num].template get<UInt8>();
    return MutableSupport::DelMark::isDel(val);
}

}
