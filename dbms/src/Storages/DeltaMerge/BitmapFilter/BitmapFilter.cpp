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

#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Segment.h>

#include <algorithm>


namespace DB::DM
{
BitmapFilter::BitmapFilter(UInt32 size_, const SegmentSnapshotPtr & snapshot_)
    : filter(size_, false)
    , snap(snapshot_)
    , all_match(false)
{}

BitmapFilter::BitmapFilter(UInt32 size_, const SegmentSnapshotPtr & snapshot_, bool all_match_)
    : filter(size_, all_match_)
    , snap(snapshot_)
    , all_match(all_match_)
{}

void BitmapFilter::set(const UInt32 * data, UInt32 start, UInt32 limit)
{
    if (limit == 0)
    {
        return;
    }
    RUNTIME_CHECK(start + limit <= filter.size(), start + limit, filter.size());
    for (UInt32 i = 0; i < limit; ++i)
    {
        UInt32 row_id = *(data + i);
        filter[row_id] = true;
    }
}

void BitmapFilter::set(const ColumnPtr & col, UInt32 start)
{
    const auto * v = toColumnVectorDataPtr<UInt32>(col);
    set(v->data(), start, v->size());
}

bool BitmapFilter::get(IColumn::Filter & f, UInt32 start, UInt32 limit) const
{
    RUNTIME_CHECK(start + limit <= filter.size(), start, limit, filter.size());
    const auto begin = filter.cbegin() + start;
    const auto end = filter.cbegin() + start + limit;
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

SegmentSnapshotPtr & BitmapFilter::snapshot()
{
    return snap;
}

void BitmapFilter::runOptimize()
{
    all_match = std::find(filter.begin(), filter.end(), false) == filter.end();
}

String BitmapFilter::toDebugString() const
{
    return fmt::format("size: {}, positive: {}", filter.size(), std::count(filter.begin(), filter.end(), true));
}

} // namespace DB::DM