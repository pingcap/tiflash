#include <Columns/ColumnsNumber.h>
#include <DataStreams/VersionFilterBlockInputStream.h>
#include <DataStreams/dedupUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

Block VersionFilterBlockInputStream::readImpl()
{
    while (true)
    {
        Block block = input->read();
        if (!block)
            return block;

        if (!block.has(MutableSupport::version_column_name))
        {
            throw Exception(
                "VersionFilterBlockInputStream: block without " + MutableSupport::version_column_name, ErrorCodes::LOGICAL_ERROR);
        }

        const ColumnWithTypeAndName & version_column = block.getByName(version_column_name);
        const ColumnUInt64 * column = static_cast<const ColumnUInt64 *>(version_column.column.get());

        size_t rows = block.rows();

        size_t pos = 0;
        bool need_filter = false;

        for (; pos < rows; ++pos)
        {
            if (column->getElement(pos) > filter_greater_version)
            {
                need_filter = true;
                break;
            }
        }

        if (!need_filter)
            return block;

        IColumn::Filter filter(rows, 1);

        size_t deleted = 0;
        for (; pos < rows; ++pos)
        {
            if (column->getElement(pos) > filter_greater_version)
            {
                deleted++;
                filter[pos] = 0;
            }
        }

        if (deleted == rows)
            continue;

        deleteRows(block, filter);
        return block;
    }
}

} // namespace DB
