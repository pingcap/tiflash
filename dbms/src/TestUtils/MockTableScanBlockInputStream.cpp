#include <TestUtils/MockTableScanBlockInputStream.h>

namespace DB
{
MockTableScanBlockInputStream::MockTableScanBlockInputStream(ColumnsWithTypeAndName columns, size_t max_block_size)
    : columns(columns)
    , output_index(0)
    , max_block_size(max_block_size)
{
    rows = 0;
    for (const auto & elem : columns)
    {
        if (elem.column)
        {
            assert(rows == 0 || rows == elem.column->size());
            rows = elem.column->size();
        }
    }
}

ColumnPtr MockTableScanBlockInputStream::makeColumn(ColumnWithTypeAndName elem)
{
    auto column = elem.type->createColumn();
    size_t row_count = 0;
    for (size_t i = output_index; i < rows & row_count < max_block_size; ++i)
    {
        column->insert((*elem.column)[i]);
        row_count++;
    }

    return column;
}

Block MockTableScanBlockInputStream::readImpl()
{
    if (output_index >= rows)
        return {};
    ColumnsWithTypeAndName output_columns;
    for (const auto & elem : columns)
    {
        output_columns.push_back({makeColumn(elem), elem.type, elem.name, elem.column_id});
    }
    output_index += max_block_size;

    return Block(output_columns);
}

} // namespace DB
