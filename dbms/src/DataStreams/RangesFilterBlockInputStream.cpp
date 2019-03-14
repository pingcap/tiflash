#include <Columns/ColumnsNumber.h>
#include <DataStreams/RangesFilterBlockInputStream.h>
#include <DataStreams/dedupUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

Block RangesFilterBlockInputStream::readImpl()
{
    while (true)
    {
        Block block = input->read();
        if (!block)
            return block;

        if (!block.has(handle_col_name))
            throw Exception("RangesFilterBlockInputStream: block without _tidb_rowid.", ErrorCodes::LOGICAL_ERROR);

        const ColumnWithTypeAndName & handle_column = block.getByName(handle_col_name);
        const ColumnInt64 * column = typeid_cast<const ColumnInt64 *>(handle_column.column.get());
        if (!column)
        {
            throw Exception("RangesFilterBlockInputStream: _tidb_rowid column should be type ColumnInt64.", ErrorCodes::LOGICAL_ERROR);
        }

        size_t rows = block.rows();

        auto handle_bg = column->getElement(0);
        auto handle_ed = column->getElement(rows - 1);

        if (handle_bg >= ranges.second || ranges.first > handle_ed)
            continue;

        if (handle_bg >= ranges.first)
        {
            if (handle_ed < ranges.second)
            {
                return block;
            }
            else
            {
                size_t pos
                    = std::lower_bound(column->getData().cbegin(), column->getData().cend(), ranges.second) - column->getData().cbegin();
                size_t pop_num = rows - pos;
                for (size_t i = 0; i < block.columns(); i++)
                {
                    ColumnWithTypeAndName & ori_column = block.getByPosition(i);
                    MutableColumnPtr mutable_holder = (*std::move(ori_column.column)).mutate();
                    mutable_holder->popBack(pop_num);
                    ori_column.column = std::move(mutable_holder);
                }
            }
        }
        else
        {
            size_t pos_bg
                = std::lower_bound(column->getData().cbegin(), column->getData().cend(), ranges.first) - column->getData().cbegin();
            size_t pos_ed = rows;
            if (handle_ed >= ranges.second)
                pos_ed = std::lower_bound(column->getData().cbegin(), column->getData().cend(), ranges.second) - column->getData().cbegin();

            size_t len = pos_ed - pos_bg;
            if (!len)
                continue;
            for (size_t i = 0; i < block.columns(); i++)
            {
                ColumnWithTypeAndName & ori_column = block.getByPosition(i);
                auto new_column = ori_column.column->cloneEmpty();
                new_column->insertRangeFrom(*ori_column.column, pos_bg, len);
                ori_column.column = std::move(new_column);
            }
        }

        return block;
    }
}

} // namespace DB
