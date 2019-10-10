#include <Columns/ColumnsNumber.h>
#include <DataStreams/RangesFilterBlockInputStream.h>
#include <DataStreams/dedupUtils.h>
#include <Storages/MergeTree/TMTMustColumns.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

struct PKColumnIterator : public std::iterator<std::random_access_iterator_tag, Int64, size_t>
{
    PKColumnIterator & operator++()
    {
        ++pos;
        return *this;
    }

    PKColumnIterator & operator=(const PKColumnIterator & itr)
    {
        pos = itr.pos;
        column = itr.column;
        return *this;
    }

    Int64 operator*() const { return column->getElement(pos); }

    size_t operator-(const PKColumnIterator & itr) const { return pos - itr.pos; }

    PKColumnIterator(const int pos_, const ColumnInt64 * column_) : pos(pos_), column(column_) {}

    void operator+=(size_t n) { pos += n; }

    size_t pos;
    const ColumnInt64 * column;
};

Block RangesFilterBlockInputStream::readImpl()
{
    while (true)
    {
        Block block = input->read();
        if (!block)
            return block;

        const ColumnWithTypeAndName & handle_column = block.getByPosition(pk_column_index);
        const ColumnInt64 * column = static_cast<const ColumnInt64 *>(handle_column.column.get());

        size_t rows = block.rows();

        auto handle_begin = column->getElement(0);
        auto handle_end = column->getElement(rows - 1);

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
                size_t pos = std::lower_bound(PKColumnIterator(0, column), PKColumnIterator(rows, column), ranges.second).pos;
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
            size_t pos_begin = std::lower_bound(PKColumnIterator(0, column), PKColumnIterator(rows, column), ranges.first).pos;
            size_t pos_end = rows;
            if (handle_end >= ranges.second)
                pos_end = std::lower_bound(PKColumnIterator(0, column), PKColumnIterator(rows, column), ranges.second).pos;

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

namespace TiKVHandle
{
const Handle Handle::normal_min = Handle(HandleIDType::NORMAL, std::numeric_limits<HandleType>::min());
const Handle Handle::max = Handle(HandleIDType::MAX, 0);
}

} // namespace DB
