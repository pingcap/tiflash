#include <Columns/ColumnsNumber.h>
#include <DataStreams/RangesFilterBlockInputStream.h>
#include <DataStreams/dedupUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

struct PKColumnIterator : public std::iterator<std::random_access_iterator_tag, UInt64, size_t>
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

    UInt64 operator*() const { return column->getUInt(pos); }

    size_t operator-(const PKColumnIterator & itr) const { return pos - itr.pos; }

    PKColumnIterator(const int pos_, const IColumn * column_) : pos(pos_), column(column_) {}

    void operator+=(size_t n) { pos += n; }

    size_t pos;
    const IColumn * column;
};

template <typename HandleType>
std::pair<size_t, size_t> RangesFilterBlockInputStream<HandleType>::findBound(const IColumn * column, HandleRange<HandleType> range, size_t rows)
{
    static const auto func_cmp = [](const UInt64 & a, const Handle & b) -> bool { return static_cast<HandleType>(a) < b; };

    auto handle_begin = static_cast<HandleType>(column->getUInt(0));
    auto handle_end = static_cast<HandleType>(column->getUInt(rows -1));

    if (handle_begin >= range.second || handle_end < range.first)
    {
        return std::make_pair(0,0);
    }
    if (handle_begin >= range.first)
    {
        if (handle_end < range.second)
        {
            return std::make_pair(0,rows);
        }
        else
        {
            size_t pos = std::lower_bound(PKColumnIterator(0, column), PKColumnIterator(rows, column),
                    range.second, func_cmp).pos;
            return std::make_pair(0, pos);
        }
    }
    else
    {
        size_t pos_begin = std::lower_bound(PKColumnIterator(0, column), PKColumnIterator(rows, column),
                range.first, func_cmp).pos;

        size_t pos_end = rows;
        if (handle_end >= range.second)
        {
            pos_end = std::lower_bound(PKColumnIterator(0, column), PKColumnIterator(rows, column),
                    range.second, func_cmp).pos;
        }
        return std::make_pair(pos_begin, pos_end);
    }
}

template <typename HandleType>
Block RangesFilterBlockInputStream<HandleType>::readImpl()
{
    bool skip_filter = isAllValueCoveredByRanges(ranges);
    while (true)
    {
        Block block = input->read();
        if (!block || skip_filter)
            return block;

        const ColumnWithTypeAndName & handle_column = block.getByPosition(handle_column_index);
        const auto * column = handle_column.column.get();

        size_t rows = block.rows();

        std::vector<std::pair<size_t, size_t>> pos_ranges;
        for (auto & range : ranges)
        {
            auto pair = findBound(column, range, rows);
            if (pair.first < pair.second) {
                pos_ranges.emplace_back(std::move(pair));
            }
        }
        if (pos_ranges.empty())
        {
            continue;
        }
        std::sort(pos_ranges.begin(), pos_ranges.end(),
                [](const std::pair<size_t, size_t> & a, const std::pair<size_t, size_t> b) { return a.first < b.first;});
        std::vector<std::pair<size_t, size_t>> merged_pos_ranges;
        size_t start = pos_ranges[0].first, end = pos_ranges[0].second;
        for (size_t i = 1; i < pos_ranges.size(); i++)
        {
            if (pos_ranges[i].first > end)
            {
                merged_pos_ranges.emplace_back(std::make_pair(start, end));
                start = pos_ranges[i].first;
                end = pos_ranges[i].second;
            }
            else
            {
                end = pos_ranges[i].second;
            }
        }
        merged_pos_ranges.emplace_back(std::make_pair(start, end));

        if (merged_pos_ranges.size() == 1)
        {
            if (merged_pos_ranges[0].first == 0 && merged_pos_ranges[0].second == rows) {
                return block;
            }
            if (merged_pos_ranges[0].first == 0)
            {
                size_t pop_num = rows - merged_pos_ranges[0].second;
                for (size_t i = 0; i < block.columns(); i++)
                {
                    ColumnWithTypeAndName &ori_column = block.getByPosition(i);
                    MutableColumnPtr mutable_holder = (*std::move(ori_column.column)).mutate();
                    mutable_holder->popBack(pop_num);
                    ori_column.column = std::move(mutable_holder);
                }
                return block;
            }
        }

        for (size_t i = 0; i < block.columns(); i++)
        {
            ColumnWithTypeAndName & org_column = block.getByPosition(i);
            auto new_column = org_column.column->cloneEmpty();
            for (auto & pair : merged_pos_ranges) {
                new_column->insertRangeFrom(*org_column.column, pair.first, pair.second - pair.first);
            }
            org_column.column = std::move(new_column);
        }
        return block;
    }
}

template class RangesFilterBlockInputStream<Int64>;
template class RangesFilterBlockInputStream<UInt64>;

} // namespace DB
