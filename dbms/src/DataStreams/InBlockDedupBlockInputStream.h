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

namespace DB
{

class InBlockDedupBlockInputStream : public IProfilingBlockInputStream
{
public:
    InBlockDedupBlockInputStream(BlockInputStreamPtr & input_, const SortDescription & description_, size_t stream_position_)
        : input(input_), description(description_), stream_position(stream_position_)
    {
        log = &Poco::Logger::get("InBlockDedupInput");
        children.emplace_back(input_);
    }

    String getName() const override
    {
        return "InBlockDedupInput";
    }

    bool isGroupedOutput() const override
    {
        return true;
    }

    bool isSortedOutput() const override
    {
        return true;
    }

    const SortDescription & getSortDescription() const override
    {
        return description;
    }

private:
    Block readImpl() override
    {
        return dedupInBlock(input->read(), description, stream_position);
    }

    Block getHeader() const override
    {
        return input->getHeader();
    }

private:
    Poco::Logger * log;
    BlockInputStreamPtr input;
    const SortDescription description;
    size_t stream_position;
};

}
