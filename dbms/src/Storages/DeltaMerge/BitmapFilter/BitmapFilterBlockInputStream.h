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

#include <Common/Stopwatch.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB::DM
{
class BitmapFilterBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "BitmapFilterBlockInputStream";

public:
    BitmapFilterBlockInputStream(
        const ColumnDefines & columns_to_read,
        SkippableBlockInputStreamPtr stable_,
        SkippableBlockInputStreamPtr delta_,
        size_t stable_rows_,
        const BitmapFilterPtr & bitmap_filter_,
        const String & req_id_);

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        FilterPtr filter_ignored;
        return readImpl(filter_ignored, false);
    }

    // When all rows in block are not filtered out,
    // `res_filter` will be set to null.
    // The caller needs to do handle this situation.
    Block readImpl(FilterPtr & res_filter, bool return_filter) override;

private:
    // When all rows in block are not filtered out,
    // `res_filter` will be set to null.
    // The caller needs to do handle this situation.
    // This function always returns the filter to the caller. It does not
    // filter the block.
    Block readImpl(FilterPtr & res_filter);

private:
    Block header;
    SkippableBlockInputStreamPtr stable;
    SkippableBlockInputStreamPtr delta;
    size_t stable_rows;
    BitmapFilterPtr bitmap_filter;
    const LoggerPtr log;
    IColumn::Filter filter{};
};

} // namespace DB::DM
