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

void BitmapFilter::set(const UInt32 * data, UInt32 size)
{
    for (UInt32 i = 0; i < size; ++i)
    {
        UInt32 row_id = *(data + i);
        RUNTIME_CHECK(row_id < filter.size(), row_id, filter.size());
        filter[row_id] = true;
    }
}

void BitmapFilter::set(const ColumnPtr & col)
{
    const auto * v = toColumnVectorDataPtr<UInt32>(col);
    set(v->data(), v->size());
}

void BitmapFilter::set(const ColumnPtr & f, UInt32 start)
{
    RUNTIME_CHECK(start + f->size() <= filter.size(), start, f->size(), filter.size());
    const auto * v = toColumnVectorDataPtr<UInt8>(f);
    std::transform(v->begin(), v->end(), filter.begin() + start, [](UInt8 v) { return v; });
}

void BitmapFilter::get(IColumn::Filter & f, UInt32 start, UInt32 limit) const
{
    RUNTIME_CHECK(start + limit <= filter.size(), start, limit, filter.size());
    if (all_match)
    {
        static const UInt8 match = 1;
        f.assign(static_cast<size_t>(limit), match);
    }
    else
    {
        std::transform(filter.begin() + start, filter.begin() + start + limit, f.begin(), [](bool v) { return v; });
    }
}

bool BitmapFilter::checkPack(UInt32 start, UInt32 limit) const
{
    RUNTIME_CHECK(start + limit <= filter.size(), start, limit, filter.size());
    return all_match || (std::find(filter.begin() + start, filter.begin() + start + limit, true) != filter.begin() + start + limit);
}

void BitmapFilter::andWith(const BitmapFilterPtr & other)
{
    if (other->all_match)
    {
        return;
    }
    RUNTIME_CHECK(filter.size() == other->filter.size(), filter.size(), other->filter.size());
    std::transform(filter.begin(), filter.end(), other->filter.begin(), filter.begin(), std::logical_and<bool>());
}

SegmentSnapshotPtr & BitmapFilter::snapshot()
{
    return snap;
}

void BitmapFilter::runOptimize()
{
    all_match = (std::find(filter.begin(), filter.end(), false) == filter.end());
}

String BitmapFilter::toDebugString() const
{
    UInt32 positive = std::count(filter.begin(), filter.end(), true);
    return fmt::format("size={}, positive={}", filter.size(), positive);
}

} // namespace DB::DM