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

#include <Common/Stopwatch.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <cstddef>

namespace DB::DM
{
class BitmapFilterBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "BitmapFilterBlockInputStream";

public:
    BitmapFilterBlockInputStream(
        const ColumnDefines & columns_to_read,
        BlockInputStreamPtr stable_,
        BlockInputStreamPtr delta_,
        const std::optional<Block> & intput_block_,
        size_t stable_rows_,
        size_t delta_rows_,
        const BitmapFilterPtr & bitmap_filter_,
        bool need_segment_col_id_,
        const String & req_id_);

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        FilterPtr filter_ignored;
        return readImpl(filter_ignored, false);
    }

    Block readImpl(FilterPtr & res_filter, bool return_filter) override;

private:
    std::pair<Block, bool> readBlock();

    Block header;
    BlockInputStreamPtr stable;
    BlockInputStreamPtr delta;
    std::optional<Block> input_block;
    size_t cur_read_rows = 0;
    size_t stable_rows;
    [[maybe_unused]] size_t delta_rows;
    BitmapFilterPtr bitmap_filter;
    bool need_segment_col_id;
    const LoggerPtr log;
    IColumn::Filter filter{};
    Stopwatch sw;
    UInt64 read_ns = 0;
    UInt64 get_filter_ns = 0;
    UInt64 filter_ns = 0;
};

} // namespace DB::DM
