#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/Transaction/TiKVHandle.h>
#include <common/logger_useful.h>

namespace DB
{

template <typename HandleType>
class RangesFilterBlockInputStream : public IProfilingBlockInputStream
{
    using Handle = TiKVHandle::Handle<HandleType>;

public:
    RangesFilterBlockInputStream(
        const BlockInputStreamPtr & input_, const std::vector<HandleRange<HandleType>> & ranges_, const size_t handle_column_index_)
        : input(input_), ranges(ranges_), handle_column_index(handle_column_index_)
    {}

protected:
    Block getHeader() const override { return input->getHeader(); }

    bool isGroupedOutput() const override { return input->isGroupedOutput(); }

    bool isSortedOutput() const override { return input->isSortedOutput(); }

    const SortDescription & getSortDescription() const override { return input->getSortDescription(); }

    String getName() const override { return "RangesFilter"; }

    std::pair<size_t, size_t> findBound(const IColumn * column, HandleRange<HandleType> range, size_t rows);

    Block readImpl() override;

private:
    BlockInputStreamPtr input;
    const std::vector<HandleRange<HandleType>> ranges;
    const size_t handle_column_index;
    Logger * log = &Logger::get("RangesFilterBlockInputStream");
};

} // namespace DB
