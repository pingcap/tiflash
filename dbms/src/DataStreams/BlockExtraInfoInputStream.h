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

/** Adds to one stream additional block information that is specified
  * as the constructor parameter.
  */
class BlockExtraInfoInputStream : public IProfilingBlockInputStream
{
public:
    BlockExtraInfoInputStream(const BlockInputStreamPtr & input, const BlockExtraInfo & block_extra_info_)
        : block_extra_info(block_extra_info_)
    {
        children.push_back(input);
    }

    BlockExtraInfo getBlockExtraInfo() const override
    {
        return block_extra_info;
    }

    String getName() const override { return "BlockExtraInfoInput"; }

    Block getHeader() const override { return children.back()->getHeader(); }

protected:
    Block readImpl() override
    {
        return children.back()->read();
    }

private:
    BlockExtraInfo block_extra_info;
};

}
