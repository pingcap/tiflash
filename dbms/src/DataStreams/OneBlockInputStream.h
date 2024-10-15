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

/** A stream of blocks from which you can read one block.
  * Also see BlocksListBlockInputStream.
  */
class OneBlockInputStream : public IProfilingBlockInputStream
{
public:
    explicit OneBlockInputStream(const Block & block_)
        : block(block_)
    {}

    String getName() const override { return "One"; }

    Block getHeader() const override
    {
        Block res;
        for (const auto & elem : block)
            res.insert({elem.column->cloneEmpty(), elem.type, elem.name});
        return res;
    }

protected:
    Block readImpl() override
    {
        if (has_been_read)
            return Block();

        has_been_read = true;
        return block;
    }

private:
    Block block;
    bool has_been_read = false;
};

} // namespace DB
