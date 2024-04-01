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

#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB::DM
{
BitmapFilter::BitmapFilter(UInt32 size_, bool default_value)
    : filter(size_, default_value)
    , all_match(default_value)
{}

void BitmapFilter::set(BlockInputStreamPtr & stream)
{
    stream->readPrefix();
    for (;;)
    {
        FilterPtr f = nullptr;
        auto blk = stream->read(f, /*res_filter*/ true);
        if (likely(blk))
        {
            set(blk.segmentRowIdCol(), f);
        }
        else
        {
            break;
        }
    }
    stream->readSuffix();
}

void BitmapFilter::set(const ColumnPtr & row_ids_col, const FilterPtr & f)
{
    const auto * v = toColumnVectorDataPtr<UInt32>(row_ids_col);
    set({v->data(), v->size()}, f);
}

void BitmapFilter::set(std::span<const UInt32> row_ids, const FilterPtr & f)
{
    if (row_ids.empty())
    {
        return;
    }
    if (!f)
    {
        for (auto row_id : row_ids)
        {
            filter[row_id] = true;
        }
    }
    else
    {
        RUNTIME_CHECK(row_ids.size() == f->size(), row_ids.size(), f->size());
        for (UInt32 i = 0; i < row_ids.size(); i++)
        {
            filter[row_ids[i]] = (*f)[i];
        }
    }
}

void BitmapFilter::set(UInt32 start, UInt32 limit, bool value)
{
    RUNTIME_CHECK(start + limit <= filter.size(), start, limit, filter.size());
    std::fill_n(filter.begin() + start, limit, value);
}

bool BitmapFilter::get(IColumn::Filter & f, UInt32 start, UInt32 limit) const
{
    RUNTIME_CHECK(start + limit <= filter.size(), start, limit, filter.size());
    auto begin = filter.cbegin() + start;
    auto end = filter.cbegin() + start + limit;
    if (all_match || std::find(begin, end, false) == end)
    {
        return true;
    }
    else
    {
        std::copy(begin, end, f.begin());
        return false;
    }
}

void BitmapFilter::rangeAnd(IColumn::Filter & f, UInt32 start, UInt32 limit) const
{
    RUNTIME_CHECK(start + limit <= filter.size() && f.size() == limit);
    auto begin = filter.cbegin() + start;
    if (!all_match)
    {
        std::transform(f.begin(), f.end(), begin, f.begin(), [](const UInt8 a, const bool b) { return a != 0 && b; });
    }
}

void BitmapFilter::runOptimize()
{
    all_match = std::find(filter.begin(), filter.end(), false) == filter.end();
}

String BitmapFilter::toDebugString() const
{
    String s(filter.size(), '1');
    for (UInt32 i = 0; i < filter.size(); i++)
    {
        if (!filter[i])
        {
            s[i] = '0';
        }
    }
    return fmt::format("{}", s);
}

size_t BitmapFilter::count() const
{
    return std::count(filter.cbegin(), filter.cend(), true);
}
} // namespace DB::DM