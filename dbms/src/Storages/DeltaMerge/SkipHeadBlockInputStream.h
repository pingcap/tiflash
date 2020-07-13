#pragma once

#include <Storages/DeltaMerge/PKFilter.h>
#include <Storages/DeltaMerge/PKRange.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB
{
namespace DM
{
class SkipHeadBlockInputStream : public SkippableBlockInputStream
{
public:
    SkipHeadBlockInputStream(const SkippableBlockInputStreamPtr & input_, PKRange & pk_range_) : input(input_), pk_range(pk_range_)
    {
        if (!pk_range.isEndInfinite())
            throw Exception("The end of pk range should be infinite for SkipHeadBlockInputStream");

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
            auto [offset, limit] = pk_range.getPosRange(block, 0, rows);
            if (unlikely(offset + limit != rows))
                throw Exception("Logical error!");

            skip_rows += offset;
            if (limit)
            {
                sk_first_block = PKFilter::cutBlock(std::move(block), offset, limit);
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

    PKRange pk_range;

    size_t sk_call_status = 0; // 0: initial, 1: called once by getSkippedRows
    Block  sk_first_block;
};

} // namespace DM
} // namespace DB