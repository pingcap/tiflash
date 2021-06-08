#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

class SimpleBlockInputStream : public IProfilingBlockInputStream
{
public:
    SimpleBlockInputStream(const BlockInputStreamPtr & input) { children.push_back(input); }

    String getName() const override { return "SimpleInputStream"; }
    Block getHeader() const override { return children.back()->getHeader(); }

protected:
    Block readImpl() override { return children.back()->read(); }
};

} // namespace DB
