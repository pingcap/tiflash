#include <TestUtils/MockTableScanBlockInputStream.h>

namespace DB
{
MockTableScanBlockInputStream::MockTableScanBlockInputStream(ColumnsWithTypeAndName columns, size_t max_block_size)
    : columns(columns)
    , output_index(0)
    , max_block_size(max_block_size)
{
}

Block MockTableScanBlockInputStream::readImpl()
{
    if (output_index >= columns.size())
        return {};
    size_t rows_count = 0;
    ColumnsWithTypeAndName output_columns;
    while (rows_count <= max_block_size && output_index < columns.size())
    {
        output_columns.push_back(columns[output_index++]);
        rows_count++;
    }
    return Block(output_columns);
}

} // namespace DB