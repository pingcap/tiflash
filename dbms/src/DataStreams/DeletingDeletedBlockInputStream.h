#pragma once

#include <common/logger_useful.h>
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

class DeletingDeletedBlockInputStream : public IProfilingBlockInputStream
{
private:
    Block readImpl() override;

    Block getHeader() const override
    {
        return input->getHeader();
    }

public:
    DeletingDeletedBlockInputStream(BlockInputStreamPtr & input_, const std::string & delmark_column_) :
        input(input_), delmark_column(delmark_column_)
    {
        log = &Logger::get(getName());
        children.emplace_back(input_);
    }

    String getName() const override
    {
        return "DeletingDeletedInput";
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
        return input->getSortDescription();
    }

private:
    Logger * log;
    BlockInputStreamPtr input;
    const std::string delmark_column;
};

}
