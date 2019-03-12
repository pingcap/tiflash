#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/Transaction/Region.h>
#include <common/logger_useful.h>

namespace DB
{

class RangesFilterBlockInputStream : public IProfilingBlockInputStream
{
public:
    RangesFilterBlockInputStream(const BlockInputStreamPtr & input_, const HandleRange & ranges_) : input(input_), ranges(ranges_) {}

protected:
    Block getHeader() const override { return input->getHeader(); }

    bool isGroupedOutput() const override { return input->isGroupedOutput(); }

    bool isSortedOutput() const override { return input->isSortedOutput(); }

    const SortDescription & getSortDescription() const override { return input->getSortDescription(); }

    String getName() const override { return "RangesFilter"; }

    Block readImpl() override;

private:
    BlockInputStreamPtr input;
    const HandleRange ranges;
    Logger * log = &Logger::get("RangesFilterBlockInputStream");
};

} // namespace DB
