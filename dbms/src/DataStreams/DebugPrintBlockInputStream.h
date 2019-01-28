#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/dedupUtils.h>

namespace DB
{

class DebugPrintBlockInputStream : public IProfilingBlockInputStream
{
private:
    Block readImpl() override
    {
        Block block = input->read();
        if (!block)
        {
            LOG_DEBUG(log, "Empty block.");
            return block;
        }
        std::stringstream writer;
        DebugPrinter::print(writer, block);
        LOG_DEBUG(log, "Block rows:\n" + writer.str());
        return block;
    }

    Block getHeader() const override
    {
        return input->getHeader();
    }

public:
    DebugPrintBlockInputStream(BlockInputStreamPtr & input_, std::string log_prefix_ = "") : input(input_)
    {
        log = &Logger::get("DebugPrintInput." + log_prefix_);
        children.emplace_back(input_);
    }

    String getName() const override
    {
        return "DebugPrintInput";
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
};

}
