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

#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/ColumnDefine_fwd.h>

namespace DB::DM
{

class BitmapFilterBlockInputStream : public IBlockInputStream
{
    static constexpr auto NAME = "BitmapFilterBlockInputStream";

public:
    BitmapFilterBlockInputStream(
        const ColumnDefines & columns_to_read,
        BlockInputStreamPtr stream_,
        const BitmapFilterPtr & bitmap_filter_,
        const String & req_id_);

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

protected:
    Block read() override;

private:
    // When all rows in block are not filtered out, `res_filter` will be set to null.
    // The caller needs to do handle this situation.
    // This function always returns the filter to the caller. It does not filter the block.
    Block readImpl(FilterPtr & res_filter);

private:
    Block header;
    // [stable..., persisted_delta, memtable]
    BlockInputStreamPtr stream;
    BitmapFilterPtr bitmap_filter;
    const LoggerPtr log;
    IColumn::Filter filter;
};

} // namespace DB::DM
