#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <common/logger_useful.h>

namespace DB
{

/// make it only can be used when pk is int64 or uint64
class TMTSingleSortedBlockInputStream : public IProfilingBlockInputStream
{
public:
    TMTSingleSortedBlockInputStream(const BlockInputStreamPtr & input_) : input(input_) {}

protected:
    Block getHeader() const override { return input->getHeader(); }

    bool isGroupedOutput() const override { return input->isGroupedOutput(); }

    bool isSortedOutput() const override { return input->isSortedOutput(); }

    const SortDescription & getSortDescription() const override { return input->getSortDescription(); }

    String getName() const override { return "TMTSingleSortedBlockInputStream"; }

    Block readImpl() override;

private:
    void updateNextBlock();

private:
    BlockInputStreamPtr input;

    bool first = true;
    bool finish = false;
    Block cur_block;
    Block next_block;
    Logger * log = &Logger::get("TMTSingleSortedBlockInputStream");
};

} // namespace DB
