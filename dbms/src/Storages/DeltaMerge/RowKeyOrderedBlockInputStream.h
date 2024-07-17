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

#include <Storages/DeltaMerge/ReadUtil.h>


namespace DB::DM
{

/**
  * RowKeyOrderedBlockInputStream is a BlockInputStream that reads from stable and delta
  * RowKeyOrderedBlockInputStream read from stable to delta, from old to new.
  */
class RowKeyOrderedBlockInputStream : public SkippableBlockInputStream
{
    static constexpr auto NAME = "RowKeyOrderedBlockInputStream";

public:
    explicit RowKeyOrderedBlockInputStream(
        const ColumnDefines & columns_to_read_,
        SkippableBlockInputStreamPtr stable_,
        SkippableBlockInputStreamPtr delta_,
        size_t stable_rows_,
        const String & req_id_)
        : header(toEmptyBlock(columns_to_read_))
        , stable(std::move(stable_))
        , delta(std::move(delta_))
        , stable_rows(stable_rows_)
        , log(Logger::get(NAME, req_id_))
    {}

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

    bool getSkippedRows(size_t & /*skip_rows*/) override
    {
        throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    size_t skipNextBlock() override { return skipBlock(stable, delta); }

    Block readWithFilter(const IColumn::Filter & filter, FilterPtr & res_filter, bool return_filter) override
    {
        auto [block, from_delta] = readBlockWithFilter(stable, delta, filter, res_filter, return_filter);
        if (block)
        {
            if (from_delta)
            {
                block.setStartOffset(block.startOffset() + stable_rows);
            }
        }
        return block;
    }

    Block read() override
    {
        auto [block, from_delta] = readBlock(stable, delta);
        if (block)
        {
            if (from_delta)
            {
                block.setStartOffset(block.startOffset() + stable_rows);
            }
        }
        return block;
    }

    Block read(FilterPtr & res_filter, bool return_filter) override
    {
        auto [block, from_delta] = readBlock(stable, delta, res_filter, return_filter);
        if (block)
        {
            if (from_delta)
            {
                block.setStartOffset(block.startOffset() + stable_rows);
            }
        }
        return block;
    }

private:
    Block header;
    SkippableBlockInputStreamPtr stable;
    SkippableBlockInputStreamPtr delta;
    size_t stable_rows;
    const LoggerPtr log;
};

} // namespace DB::DM
