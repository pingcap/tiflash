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
/** Combines several sources into one.
  * Unlike UnionBlockInputStream, it does this sequentially.
  * Blocks of different sources are not interleaved with each other.
  */
class ConcatBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "Concat";

public:
    ConcatBlockInputStream(BlockInputStreams inputs_, const String & req_id)
        : log(Logger::get(req_id))
    {
        children.insert(children.end(), inputs_.begin(), inputs_.end());
        current_stream = children.begin();
    }

    String getName() const override { return NAME; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override
    {
        FilterPtr filter_ignored;
        return readImpl(filter_ignored, false);
    }

    Block readImpl(FilterPtr & res_filter, bool return_filter) override
    {
        Block res;

        while (current_stream != children.end())
        {
            res = (*current_stream)->read(res_filter, return_filter);

            if (res)
                break;
            else
            {
                (*current_stream)->readSuffix();
                ++current_stream;
            }
        }

        return res;
    }

private:
    BlockInputStreams::iterator current_stream;

    const LoggerPtr log;
};

} // namespace DB
