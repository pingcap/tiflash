#include <Columns/ColumnsNumber.h>
#include <DataStreams/RangesFilterBlockInputStream.h>
#include <DataStreams/dedupUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template<typename T, typename ELEM_T>
Block RangesFilterBlockInputStream::readProcess(Block & block, T column, ELEM_T)
{
    size_t rows = block.rows();

    auto handle_begin = column->getElement(0);
    auto handle_end = column->getElement(rows - 1);
    constexpr HandleID min = std::numeric_limits<HandleID>::min();
    constexpr HandleID max = std::numeric_limits<HandleID>::max();
    HandleID range_start = ranges.first;
    HandleID range_end = ranges.second;
    if (range_start == min)
    {
        range_start = std::numeric_limits<ELEM_T>::min();
    }
    if (range_end == max)
    {
        range_end = std::numeric_limits<ELEM_T>::max();
    }

    if (handle_begin >= static_cast<ELEM_T>(range_end) || static_cast<ELEM_T>(range_start) > handle_end)
        return block.cloneEmpty();

    if (handle_begin >= static_cast<ELEM_T>(range_start))
    {
        if (handle_end < static_cast<ELEM_T>(range_end))
        {
            return block;
        }
        else
        {
            size_t pos
                = std::lower_bound(column->getData().cbegin(), column->getData().cend(), range_end) - column->getData().cbegin();
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
            = std::lower_bound(column->getData().cbegin(), column->getData().cend(), range_start) - column->getData().cbegin();
        size_t pos_end = rows;
        if (handle_end >= static_cast<ELEM_T>(range_end))
            pos_end = std::lower_bound(column->getData().cbegin(), column->getData().cend(), range_end) - column->getData().cbegin();

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
        std::string handle_column_type_name(handle_column.type->getFamilyName());
        if (handle_column_type_name == "Int64")
        {
            block = readProcess(block, typeid_cast<const ColumnInt64 *>(handle_column.column.get()), (ColumnInt64::value_type)0);
        } else if (handle_column_type_name == "Int32")
        {
            block = readProcess(block, typeid_cast<const ColumnInt32 *>(handle_column.column.get()), (ColumnInt32::value_type)0);
        } else if (handle_column_type_name == "Int16")
        {
            block = readProcess(block, typeid_cast<const ColumnInt16 *>(handle_column.column.get()), (ColumnInt16::value_type)0);
        } else if (handle_column_type_name == "Int8")
        {
            block = readProcess(block, typeid_cast<const ColumnInt8 *>(handle_column.column.get()), (ColumnInt8::value_type)0);
        } else if (handle_column_type_name == "UInt64")
        {
            block = readProcess(block, typeid_cast<const ColumnUInt64 *>(handle_column.column.get()), (ColumnUInt64::value_type)0);
        } else if (handle_column_type_name == "UInt32")
        {
            block = readProcess(block, typeid_cast<const ColumnUInt32 *>(handle_column.column.get()), (ColumnUInt32::value_type)0);
        } else if (handle_column_type_name == "UInt16")
        {
            block = readProcess(block, typeid_cast<const ColumnUInt16 *>(handle_column.column.get()), (ColumnUInt16::value_type)0);
        } else if (handle_column_type_name == "UInt8")
        {
            block = readProcess(block, typeid_cast<const ColumnUInt8 *>(handle_column.column.get()), (ColumnUInt8::value_type)0);
        }
        else
        {
            throw Exception("RangesFilterBlockInputStream: handle column should be type ColumnInt64, ColumnInt32, ColumnInt16 or ColumnInt8.");
        }
        if (block.rows() == 0)
        {
            continue;
        }
        return block;
    }
}

} // namespace DB
