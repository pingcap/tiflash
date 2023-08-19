// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    DebugPrintBlockInputStream(BlockInputStreamPtr & input_, std::string log_prefix_ = "")
        : input(input_)
    {
        log = &Poco::Logger::get("DebugPrintInput." + log_prefix_);
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
    Poco::Logger * log;
    BlockInputStreamPtr input;
};

} // namespace DB
