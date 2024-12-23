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

#include <Columns/IColumn.h>
#include <DataStreams/IBlockInputStream.h>

#include <span>

namespace DB::DM
{

class BitmapFilter
{
public:
    BitmapFilter(UInt32 size_, bool default_value);

    // Read blocks from `stream` and set the rows_id to be true according to the
    // `segmentRowIdCol` in the block read from `stream`.
    void set(BlockInputStreamPtr & stream);
    // f[start, satrt+limit) = value
    void set(UInt32 start, UInt32 limit, bool value = true);
    // If return true, all data is match and do not fill the filter.
    bool get(IColumn::Filter & f, UInt32 start, UInt32 limit) const;
    // Caller should ensure n in [0, size).
    inline bool get(UInt32 n) const { return filter[n]; }
    // filter[start, satrt+limit) & f -> f
    void rangeAnd(IColumn::Filter & f, UInt32 start, UInt32 limit) const;

    void runOptimize();

    String toDebugString() const;
    size_t count() const;
    inline size_t size() const { return filter.size(); }

    friend class BitmapFilterView;

private:
    void set(std::span<const UInt32> row_ids, const FilterPtr & f);

    IColumn::Filter filter;
    bool all_match;
};

using BitmapFilterPtr = std::shared_ptr<BitmapFilter>;
} // namespace DB::DM
