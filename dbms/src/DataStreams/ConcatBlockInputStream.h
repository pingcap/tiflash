#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Mpp/getMPPTaskLog.h>


namespace DB
{
/** Combines several sources into one.
  * Unlike UnionBlockInputStream, it does this sequentially.
  * Blocks of different sources are not interleaved with each other.
  */
class ConcatBlockInputStream : public IProfilingBlockInputStream
{
public:
    ConcatBlockInputStream(BlockInputStreams inputs_, const LogWithPrefixPtr & log_)
        : log(getMPPTaskLog(log_, getNameImpl()))
    {
        children.insert(children.end(), inputs_.begin(), inputs_.end());
        current_stream = children.begin();
    }

    String getName() const override { return getNameImpl(); }

    // Add this function because static analysis forbids calling virtual function in constructor
    inline String getNameImpl() const { return "Concat"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override
    {
        FilterPtr filter;
        return readImpl(filter, false);
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

    LogWithPrefixPtr log;
};

} // namespace DB
