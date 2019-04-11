#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/Transaction/Region.h>
#include <common/logger_useful.h>

namespace DB
{

class RangesFilterBlockInputStream : public IProfilingBlockInputStream
{
public:
    RangesFilterBlockInputStream(const BlockInputStreamPtr & input_, const HandleRange & ranges_, const String & handle_col_name_) : input(input_), ranges(ranges_), handle_col_name(handle_col_name_) {}

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
    const String handle_col_name;
    Logger * log = &Logger::get("RangesFilterBlockInputStream");

    template<typename T, typename ELEM_T>
    Block readProcess(Block & block, T column, ELEM_T elem);
};

} // namespace DB
