#include <Columns/ColumnsNumber.h>
#include <DataStreams/dedupUtils.h>
#include <DataStreams/VersionFilterBlockInputStream.h>

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
            throw Exception("VersionFilterBlockInputStream: block without version_column_name.",
                    ErrorCodes::LOGICAL_ERROR);
        }

        const ColumnWithTypeAndName & version_column = block.getByName(version_column_name);
        const ColumnUInt64 * column = typeid_cast<const ColumnUInt64 *>(version_column.column.get());
        if (!column)
        {
            throw Exception("VersionFilterBlockInputStream: Version column should be type ColumnUInt64.",
                    ErrorCodes::LOGICAL_ERROR);
        }

        size_t rows = block.rows();
        IColumn::Filter filter(rows, 1);

        size_t deleted = 0;
        for (size_t i = 0; i < rows; i++)
        {
            if (column->getElement(i) > filter_greater_version)
            {
                deleted++;
                filter[i] = 0;
            }
        }

        // REVIEW: optimize `deleted == 0`
        if (deleted == rows)
            continue;

        deleteRows(block, filter);
        return block;
    }
}

}
