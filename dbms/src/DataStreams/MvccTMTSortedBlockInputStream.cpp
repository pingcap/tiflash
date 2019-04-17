#include <DataStreams/MvccTMTSortedBlockInputStream.h>
#include <Storages/MutableSupport.h>
#include <Columns/ColumnsNumber.h>

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

        if (auto cur_tso = static_cast<const ColumnUInt64 *>(current->all_columns[version_column_number])->getElement(current->pos); cur_tso <= read_tso)
        {
            if (selected_row.empty() || cur_tso > static_cast<const ColumnUInt64 *>((*selected_row.columns)[version_column_number])->getElement(selected_row.row_num))
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
    const ColumnUInt8 * column = static_cast<const ColumnUInt8 *>((*selected_row.columns)[del_column_number]);
    return MutableSupport::DelMark::isDel(column->getElement(selected_row.row_num));
}

}
