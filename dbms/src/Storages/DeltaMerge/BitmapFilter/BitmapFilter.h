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
    BitmapFilter(std::initializer_list<UInt8> init);

    // Read blocks from `stream` and set the rows_id to be true according to the
    // `segmentRowIdCol` in the block read from `stream`.
    void set(BlockInputStreamPtr & stream);
    // f[start, start+limit) = value
    void set(UInt32 start, UInt32 limit, bool value = true);
    void set(std::span<const UInt32> row_ids, const FilterPtr & f);
    // If return true, all data is match and do not fill the filter.
    bool get(IColumn::Filter & f, UInt32 start, UInt32 limit) const;
    // Caller should ensure n in [0, size).
    inline bool get(UInt32 n) const { return filter[n]; }
    // filter[start, start+limit) & f -> f
    void rangeAnd(IColumn::Filter & f, UInt32 start, UInt32 limit) const;

    // f = f | other
    void logicalOr(const BitmapFilter & other);
    // f = f & other
    void logicalAnd(const BitmapFilter & other);
    // f = f + other
    void append(const BitmapFilter & other);
    // all_of(filter[start, start+limit), false)
    bool isAllNotMatch(size_t start, size_t limit) const;

    void runOptimize();
    void setAllMatch(bool all_match_) { all_match = all_match_; }
    bool isAllMatch() const { return all_match; }

    String toDebugString() const;
    size_t count() const;
    inline size_t size() const { return filter.size(); }

    ALWAYS_INLINE auto & operator[](size_t n) { return filter[n]; }

    bool operator==(const BitmapFilter & other) const { return filter == other.filter && all_match == other.all_match; }

    friend class BitmapFilterView;

private:
    IColumn::Filter filter;
    bool all_match;
};

using BitmapFilterPtr = std::shared_ptr<BitmapFilter>;
} // namespace DB::DM
