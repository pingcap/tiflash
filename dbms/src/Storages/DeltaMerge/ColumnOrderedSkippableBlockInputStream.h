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

#include <Storages/DeltaMerge/ReadUtil.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>


namespace DB::DM
{

class ColumnOrderedSkippableBlockInputStream : public SkippableBlockInputStream
{
    static constexpr auto NAME = "ColumnOrderedSkippableBlockInputStream";

public:
    explicit ColumnOrderedSkippableBlockInputStream(
        const ColumnDefines & columns_to_read_,
        SkippableBlockInputStreamPtr stable_,
        SkippableBlockInputStreamPtr delta_,
        size_t stable_rows_,
        const String & req_id_)
        : header(toEmptyBlock(columns_to_read_))
        , stable(stable_)
        , delta(delta_)
        , stable_rows(stable_rows_)
        , log(Logger::get(NAME, req_id_))
    {}

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

    bool getSkippedRows(size_t & skip_rows) override
    {
        if (cur_read_rows > stable_rows)
        {
            return false;
        }
        return stable->getSkippedRows(skip_rows);
    }

    bool skipNextBlock() override
    {
        return skipBlock(stable, delta);
    }

    Block read() override
    {
        // auto inner_stable = static_cast<BlockInputStreamPtr>(stable);
        auto [block, from_delta] = readBlock(stable, delta);
        if (block)
        {
            if (from_delta)
            {
                block.setStartOffset(block.startOffset() + stable_rows);
            }
            cur_read_rows += block.rows();
        }
        return block;
    }

private:
    Block header;
    SkippableBlockInputStreamPtr stable;
    SkippableBlockInputStreamPtr delta;
    size_t stable_rows;
    size_t cur_read_rows = 0;
    const LoggerPtr log;
    IColumn::Filter filter{};
};

} // namespace DB::DM