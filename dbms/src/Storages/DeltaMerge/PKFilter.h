#pragma once


#include <Columns/ColumnConst.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/PKRange.h>

namespace DB
{
namespace DM
{

namespace PKFilter
{

/// return <offset, limit>
inline std::pair<size_t, size_t>
getPosRangeOfSorted(const PKRange & pk_range, const Columns & pk_data, const size_t offset, const size_t limit)
{
    return pk_range.getPosRange(pk_data, offset, limit);
}

inline Block cutBlock(Block && block, size_t offset, size_t limit)
{
    size_t rows = block.rows();
    if (!limit)
        return {};
    if (offset == 0 && limit == rows)
        return std::move(block);

    if (offset == 0)
    {
        size_t pop_size = rows - limit;
        for (size_t i = 0; i < block.columns(); i++)
        {
            auto & column     = block.getByPosition(i);
            auto   mutate_col = (*std::move(column.column)).mutate();
            mutate_col->popBack(pop_size);
            column.column = std::move(mutate_col);
        }
    }
    else
    {
        for (size_t i = 0; i < block.columns(); i++)
        {
            auto & column     = block.getByPosition(i);
            auto   new_column = column.column->cloneEmpty();
            new_column->insertRangeFrom(*column.column, offset, limit);
            column.column = std::move(new_column);
        }
    }
    return std::move(block);
}

inline Block filterSorted(const PKRange & pk_range, Block && block)
{
    auto [offset, limit] = pk_range.getPosRange(block, 0, block.rows());
    return cutBlock(std::move(block), offset, limit);
}

inline Block filterUnsorted(const PKRange & pk_range, Block && block)
{
    size_t rows = block.rows();

    IColumn::Filter filter(rows);
    size_t          passed_count = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        bool ok   = pk_range.check(block, i);
        filter[i] = ok;
        passed_count += ok;
    }

    if (!passed_count)
        return {};
    if (passed_count == rows)
        return std::move(block);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & column = block.getByPosition(i);
        column.column = column.column->filter(filter, passed_count);
    }
    return std::move(block);
}
} // namespace PKFilter

template <bool is_block_sorted>
class DMHandleFilterBlockInputStream : public IBlockInputStream
{
public:
    DMHandleFilterBlockInputStream(const BlockInputStreamPtr & input, const PKRange & pk_range_) : pk_range(pk_range_)
    {
        children.push_back(input);
    }

    String getName() const override { return "DeltaMergeHandleFilter"; }
    Block  getHeader() const override { return children.back()->getHeader(); }

    Block read() override
    {
        while (true)
        {
            Block block = children.back()->read();
            if (!block)
                return {};
            if (!block.rows())
                continue;

            bool is_const = true;
            for (size_t i = 0; i < pk_range.primaryKey()->size(); i++)
            {
                auto col = block.getByPosition(i).column;
                if (!col->isColumnConst())
                {
                    is_const = false;
                    break;
                }
            }
            if (is_const)
            {
                if (pk_range.check(block, 0))
                    return block;
                else
                    return {};
            }

            Block res = is_block_sorted ? PKFilter::filterSorted(pk_range, std::move(block))
                                        : PKFilter::filterUnsorted(pk_range, std::move(block));
            if (!res || !res.rows())
                continue;
            else
                return res;
        }
    }

private:
    PKRange pk_range;
};

} // namespace DM
} // namespace DB
