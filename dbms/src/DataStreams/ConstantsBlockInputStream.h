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

#include <Common/Exception.h>
#include <DataStreams/IBlockInputStream.h>

namespace DB
{
class ConstantsBlockInputStream : public IBlockInputStream
{
public:
    ConstantsBlockInputStream(const Block & header, size_t rows_)
        : header(header)
        , rows(rows_)
    {
        for (const auto & col : header)
        {
            RUNTIME_CHECK(col.column != nullptr && col.column->isColumnConst());
        }
        if unlikely (rows == 0 || header.columns() == 0)
            done = true;
    }

    Block read() override
    {
        if unlikely (done)
            return {};

        done = true;
        Block block = header;
        for (auto & col : block)
        {
            col.column = col.column->cloneResized(rows);
        }
        return block;
    }
    Block getHeader() const override { return header; }
    String getName() const override { return "Constants"; }

private:
    Block header;
    size_t rows;

    bool done = false;
};
} // namespace DB
