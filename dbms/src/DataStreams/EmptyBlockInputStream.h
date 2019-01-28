#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

/**
 * An input stream always return empty block.
 */
class EmptyBlockInputStream : public IProfilingBlockInputStream
{
public:
    EmptyBlockInputStream(const Block & header_) : header(header_) {}

    String getName() const override
    {
        return "Empty";
    }

    Block getHeader() const override
    {
        return header;
    }

protected:
    Block readImpl() override
    {
        return {};
    }

private:
    Block header;
};
}
