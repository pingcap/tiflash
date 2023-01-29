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

#include <Columns/IColumn.h>
#include <DataStreams/IBlockInputStream.h>

namespace DB::DM
{

class BitmapFilter
{
public:
    BitmapFilter(UInt32 size_, bool default_value);

    void set(BlockInputStreamPtr & stream);
    void set(const ColumnPtr & col, const FilterPtr & f);
    void set(const UInt32 * data, UInt32 size, const FilterPtr & f);
    void set(UInt32 start, UInt32 limit);
    // If return true, all data is match and do not fill the filter.
    bool get(IColumn::Filter & f, UInt32 start, UInt32 limit) const;

    void runOptimize();

    String toDebugString() const;
    size_t count() const;

private:
    std::vector<bool> filter;
    bool all_match;
};

using BitmapFilterPtr = std::shared_ptr<BitmapFilter>;
} // namespace DB::DM