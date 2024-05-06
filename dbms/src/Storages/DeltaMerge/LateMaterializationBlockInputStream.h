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
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB::DM
{

/** BlockInputStream to do late materialization.
  * 1. Read one block of the filter column.
  * 2. Run pushed down filter on the block, return block and filter.
  * 3. Read one block of the rest columns, join the two block by columns, and assign the filter to the returned block before return.
  * 4. Repeat 1-3 until the filter column stream is empty.
  */
class LateMaterializationBlockInputStream : public IBlockInputStream
{
    static constexpr auto NAME = "LateMaterializationBlockInputStream";

public:
    LateMaterializationBlockInputStream(
        const ColumnDefines & columns_to_read,
        const String & filter_column_name_,
        BlockInputStreamPtr filter_column_stream_,
        SkippableBlockInputStreamPtr rest_column_stream_,
        const BitmapFilterPtr & bitmap_filter_,
        const String & req_id_);

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

    Block read() override;

private:
    Block header;
    // The name of the tmp filter column in filter_column_block which is added by the FilterBlockInputStream.
    // The column is used to filter the block, but it is not included in the returned block.
    const String & filter_column_name;
    // The stream used to read the filter column, and filter the block.
    BlockInputStreamPtr filter_column_stream;
    // The stream used to read the rest columns.
    SkippableBlockInputStreamPtr rest_column_stream;
    // The MVCC-bitmap.
    BitmapFilterPtr bitmap_filter;

    const LoggerPtr log;
};

} // namespace DB::DM
