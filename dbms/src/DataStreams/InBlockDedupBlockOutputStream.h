// Copyright 2022 PingCAP, Ltd.
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
        log = &Poco::Logger::get("InBlockDedupBlockInputStream");
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
    Poco::Logger * log;
};

}
