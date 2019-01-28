#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/dedupUtils.h>

namespace DB
{

class PlaygroundBlockInputStream : public IProfilingBlockInputStream
{
private:
    Block readImpl() override
    {
        Block block = input->read();
        if (!block)
            return block;

        Block head;
        Block tail;
        SortCursorImpl key(block, description);
        splitBlockByGreaterThanKey(description, key, block, head, tail);
        return tail;
    }

public:
    PlaygroundBlockInputStream(BlockInputStreamPtr & input_, const SortDescription & description_) :
        input(input_), description(description_)
    {
        log = &Logger::get("PlaygroundInput");
        children.emplace_back(input_);
    }

    String getName() const override
    {
        return "PlaygroundInput";
    }

    bool isGroupedOutput() const override
    {
        return input->isGroupedOutput();
    }

    bool isSortedOutput() const override
    {
        return input->isSortedOutput();
    }

    const SortDescription & getSortDescription() const override
    {
        return description;
    }

private:
    Logger * log;
    BlockInputStreamPtr input;
    const SortDescription description;
};

}
