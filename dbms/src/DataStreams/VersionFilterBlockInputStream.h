#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <common/logger_useful.h>
#include <Storages/MergeTree/TMTMustColumns.h>

namespace DB
{

class VersionFilterBlockInputStream : public IProfilingBlockInputStream
{
public:
    VersionFilterBlockInputStream(const BlockInputStreamPtr & input_, UInt64 filter_greater_version_)
        : input(input_), filter_greater_version(filter_greater_version_)
    {}

protected:
    Block getHeader() const override { return input->getHeader(); }

    bool isGroupedOutput() const override { return input->isGroupedOutput(); }

    bool isSortedOutput() const override { return input->isSortedOutput(); }

    const SortDescription & getSortDescription() const override { return input->getSortDescription(); }

    String getName() const override { return "VersionFilter"; }

    Block readImpl() override;

private:
    BlockInputStreamPtr input;
    const UInt64 filter_greater_version;
    Logger * log = &Logger::get("VersionFilterBlockInputStream");
};

} // namespace DB
