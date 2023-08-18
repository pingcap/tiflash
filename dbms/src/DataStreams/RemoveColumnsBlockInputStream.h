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

/** Removes the specified columns from the block.
    */
class RemoveColumnsBlockInputStream : public IProfilingBlockInputStream
{
public:
    RemoveColumnsBlockInputStream(BlockInputStreamPtr input_, const Names & columns_to_remove_) : columns_to_remove(columns_to_remove_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "RemoveColumns"; }

protected:
    Block getHeader() const override
    {
        Block res = children.back()->getHeader();
        if (!res)
            return res;

        for (const auto & name : columns_to_remove)
            if (res.has(name))
                res.erase(name);

        return res;
    }

    Block readImpl() override
    {
        Block res = children.back()->read();
        if (!res)
            return res;

        for (const auto & name : columns_to_remove)
            if (res.has(name))
                res.erase(name);

        return res;
    }

private:
    Names columns_to_remove;
};

} // namespace DB
