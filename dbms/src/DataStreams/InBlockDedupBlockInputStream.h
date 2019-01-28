#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

class InBlockDedupBlockInputStream : public IProfilingBlockInputStream
{
public:
    InBlockDedupBlockInputStream(BlockInputStreamPtr & input_, const SortDescription & description_, size_t stream_position_)
        : input(input_), description(description_), stream_position(stream_position_)
    {
        log = &Logger::get("InBlockDedupInput");
        children.emplace_back(input_);
    }

    String getName() const override
    {
        return "InBlockDedupInput";
    }

    bool isGroupedOutput() const override
    {
        return true;
    }

    bool isSortedOutput() const override
    {
        return true;
    }

    const SortDescription & getSortDescription() const override
    {
        return description;
    }

private:
    Block readImpl() override
    {
        return dedupInBlock(input->read(), description, stream_position);
    }

    Block getHeader() const override
    {
        return input->getHeader();
    }

private:
    Logger * log;
    BlockInputStreamPtr input;
    const SortDescription description;
    size_t stream_position;
};

}
