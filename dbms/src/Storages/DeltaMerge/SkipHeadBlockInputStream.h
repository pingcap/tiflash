#pragma once

#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB
{
namespace DM
{
class SkipHeadBlockInputStream : public SkippableBlockInputStream
{
public:
    SkipHeadBlockInputStream(const SkippableBlockInputStreamPtr & input_, RowKeyRange rowkey_range_, size_t handle_col_pos_)
        : input(input_), rowkey_range(rowkey_range_), handle_col_pos(handle_col_pos_)
    {
        if (rowkey_range.isEndInfinite())
            throw Exception("The end of rowkey range should be +Inf for SkipHeadBlockInputStream");

        children.push_back(input);
    }


    String getName() const override { return "SkipHead"; }
    Block  getHeader() const override { return children.back()->getHeader(); }

    bool getSkippedRows(size_t & skip_rows) override
    {
        if (sk_call_status != 0)
            throw Exception("Call #getSkippedRows() more than once");
        ++sk_call_status;

        input->getSkippedRows(skip_rows);

        Block block;
        while ((block = children.back()->read()))
        {
            auto rows            = block.rows();
            auto [offset, limit] = RowKeyFilter::getPosRangeOfSorted(rowkey_range, block.getByPosition(handle_col_pos).column, 0, rows);
            if (unlikely(offset + limit != rows))
                throw Exception("Logical error!");

            skip_rows += offset;
            if (limit)
            {
                sk_first_block = RowKeyFilter::cutBlock(std::move(block), offset, limit);
                break;
            }
        }
        return true;
    }

    Block read() override
    {
        if (sk_call_status == 0)
            throw Exception("Unexpected call #read() in status 0");
        if (sk_call_status == 1)
        {
            Block tmp;
            tmp.swap(sk_first_block);
            ++sk_call_status;
            return tmp;
        }
        return children.back()->read();
    }

private:
    SkippableBlockInputStreamPtr input;

    RowKeyRange rowkey_range;
    size_t      handle_col_pos;

    size_t sk_call_status = 0; // 0: initial, 1: called once by getSkippedRows
    Block  sk_first_block;
};

} // namespace DM
} // namespace DB