#include <Columns/ColumnsNumber.h>
#include <DataStreams/RangesFilterBlockInputStream.h>
#include <DataStreams/dedupUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template<typename T>
Block RangesFilterBlockInputStream::readProcess(Block & block, T column)
{
    size_t rows = block.rows();

    auto handle_begin = column->getElement(0);
    auto handle_end = column->getElement(rows - 1);

    if (handle_begin >= ranges.second || ranges.first > handle_end)
        return block.cloneEmpty();

    if (handle_begin >= ranges.first)
    {
        if (handle_end < ranges.second)
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
        size_t pos_begin
            = std::lower_bound(column->getData().cbegin(), column->getData().cend(), ranges.first) - column->getData().cbegin();
        size_t pos_end = rows;
        if (handle_end >= ranges.second)
            pos_end = std::lower_bound(column->getData().cbegin(), column->getData().cend(), ranges.second) - column->getData().cbegin();

        size_t len = pos_end - pos_begin;
        if (!len)
            return block.cloneEmpty();
        for (size_t i = 0; i < block.columns(); i++)
        {
            ColumnWithTypeAndName & ori_column = block.getByPosition(i);
            auto new_column = ori_column.column->cloneEmpty();
            new_column->insertRangeFrom(*ori_column.column, pos_begin, len);
            ori_column.column = std::move(new_column);
        }
    }

    return block;
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
        std::string handle_column_type_name = std::string(handle_column.type->getFamilyName());
        if (handle_column_type_name == "Int64")
        {
            block = readProcess(block, typeid_cast<const ColumnInt64 *>(handle_column.column.get()));
        } else if (handle_column_type_name == "Int32")
        {
            block = readProcess(block, typeid_cast<const ColumnInt32 *>(handle_column.column.get()));
        } else
        {
            throw Exception("RangesFilterBlockInputStream: handle column should be type ColumnInt64 or ColumnInt32.");
        }
        if (block.rows() == 0)
        {
            continue;
        }
        return block;
    }
}

} // namespace DB
