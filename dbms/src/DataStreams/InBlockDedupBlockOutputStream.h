#pragma once

#include <DataStreams/DedupSortedBlockInputStream.h>
#include <common/logger_useful.h>

namespace DB
{

class InBlockDedupBlockOutputStream : public IBlockOutputStream
{
public:
    InBlockDedupBlockOutputStream(BlockOutputStreamPtr & output_, const SortDescription & description_)
        : output(output_), description(description_)
    {
        log = &Logger::get("InBlockDedupBlockInputStream");
    }

    void write(const Block & block) override
    {
        // TODO: Use origin CursorImpl to compare, will be faster
        Block deduped = dedupInBlock(block, description);
        output->write(deduped);
    }

    Block getHeader() const override
    {
        return output->getHeader();
    }

private:
    BlockOutputStreamPtr output;
    const SortDescription description;
    Logger * log;
};

}
