#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


/** Combines several sources into one.
  * Unlike UnionBlockInputStream, it does this sequentially.
  * Blocks of different sources are not interleaved with each other.
  */
class ConcatBlockInputStream : public IProfilingBlockInputStream
{
public:
    ConcatBlockInputStream(BlockInputStreams inputs_)
    {
        children.insert(children.end(), inputs_.begin(), inputs_.end());
        current_stream = children.begin();
    }

    String getName() const override { return "Concat"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override
    {
        FilterPtr filter_;
        return readImpl(filter_, false);
    }

    Block readImpl(FilterPtr & res_filter, bool return_filter) override
    {
        Block res;

        while (current_stream != children.end())
        {
            res = (*current_stream)->read(res_filter, return_filter);

            if (res)
                break;
            else
            {
                (*current_stream)->readSuffix();
                ++current_stream;
            }
        }

        return res;
    }

private:
    BlockInputStreams::iterator current_stream;
};

} // namespace DB
