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

/** A stream of blocks from which you can read the next block from an explicitly provided list.
  * Also see OneBlockInputStream.
  */
class BlocksListBlockInputStream : public IProfilingBlockInputStream
{
public:
    /// Acquires the ownership of the block list.
    BlocksListBlockInputStream(BlocksList && list_)
        : list(std::move(list_))
        , it(list.begin())
        , end(list.end())
    {}

    /// Uses a list of blocks lying somewhere else.
    BlocksListBlockInputStream(const BlocksList::iterator & begin_, const BlocksList::iterator & end_)
        : it(begin_)
        , end(end_)
    {}

    String getName() const override { return "BlocksList"; }

    Block getHeader() const override
    {
        Block res;
        if (!list.empty())
            for (const auto & elem : list.front())
                res.insert({elem.column->cloneEmpty(), elem.type, elem.name, elem.column_id});
        return res;
    }

protected:
    Block readImpl() override
    {
        if (it == end)
            return Block();

        Block res = *it;
        ++it;
        return res;
    }

private:
    BlocksList list;
    BlocksList::iterator it;
    const BlocksList::iterator end;
};

} // namespace DB
