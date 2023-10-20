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

#include <Common/Exception.h>
#include <DataStreams/IBlockInputStream.h>

namespace DB
{
class ConstantsBlockInputStream : public IBlockInputStream
{
public:
    ConstantsBlockInputStream(const Block & header, UInt64 rows_, UInt64 max_block_size_)
        : header(header)
        , remaining_rows(rows_)
        , max_block_size(std::max(1, max_block_size_))
    {
        RUNTIME_CHECK_MSG(header.columns() > 0, "the empty header is illegal.");
        for (const auto & col : header)
        {
            RUNTIME_CHECK(col.column != nullptr && col.column->isColumnConst());
        }
    }

    Block read() override
    {
        if unlikely (remaining_rows == 0)
            return {};

        size_t cur_rows = std::min(max_block_size, remaining_rows);
        remaining_rows -= cur_rows;
        Block block = header;
        for (auto & col : block)
        {
            col.column = col.column->cloneResized(cur_rows);
        }
        return block;
    }
    Block getHeader() const override { return header; }
    String getName() const override { return "Constants"; }

private:
    const Block header;
    UInt64 remaining_rows;
    const UInt64 max_block_size;
};
} // namespace DB
