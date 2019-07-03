#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

class TMTUnionBlockInputStream : public IProfilingBlockInputStream
{
public:
    TMTUnionBlockInputStream(BlockInputStreams inputs_)
    {
        children = std::move(inputs_);
        it = children.begin();
    }

    String getName() const override { return "TMTUnion"; }

protected:
    Block getHeader() const override { return children.back()->getHeader(); }

    Block readImpl() override
    {
        for (; it != children.end(); it++)
        {
            Block res = (*it)->read();
            if (res)
                return res;
        }

        return {};
    }

private:
    BlockInputStreams::iterator it;
};

} // namespace DB
