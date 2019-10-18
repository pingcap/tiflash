#include <Columns/ColumnsNumber.h>
#include <DataStreams/RangesFilterBlockInputStream.h>
#include <DataStreams/dedupUtils.h>
#include <DataStreams/PKColumnIterator.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template <typename HandleType>
Block RangesFilterBlockInputStream<HandleType>::readImpl()
{
    static const auto func_cmp = PkCmp<HandleType>;

    while (true)
    {
        Block block = input->read();
        if (!block)
            return block;

        const ColumnWithTypeAndName & handle_column = block.getByPosition(handle_column_index);
        const auto * column = handle_column.column.get();

        size_t rows = block.rows();

        auto handle_begin = static_cast<HandleType>(column->getUInt(0));
        auto handle_end = static_cast<HandleType>(column->getUInt(rows - 1));

        if (handle_begin >= ranges.second || ranges.first > handle_end)
            continue;

        if (handle_begin >= ranges.first)
        {
            if (handle_end < ranges.second)
            {
                return block;
            }
            else
            {
                size_t pos = std::lower_bound(PKColumnIterator(0, column), PKColumnIterator(rows, column), ranges.second, func_cmp).pos;
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
            size_t pos_begin = std::lower_bound(PKColumnIterator(0, column), PKColumnIterator(rows, column), ranges.first, func_cmp).pos;
            size_t pos_end = rows;
            if (handle_end >= ranges.second)
                pos_end = std::lower_bound(PKColumnIterator(0, column), PKColumnIterator(rows, column), ranges.second, func_cmp).pos;

            size_t len = pos_end - pos_begin;
            if (!len)
                continue;
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
}

template class RangesFilterBlockInputStream<Int64>;
template class RangesFilterBlockInputStream<UInt64>;

} // namespace DB
